/**
 * Telegram Bot Worker v3.70+ 
 * 架构: Cloudflare Workers + D1 Database
 * 特性:
 *  - Mini App 验证：initData 验签 + 强绑定 userId
 *  - 红蓝对抗加固：严格管理员鉴权、过期锁、防崩溃正则、user_info 合并更新
 *  - 转发成功后：用户侧用 👍 表情 标记“已送达”（失败兜底为“✅ 已送达”）
 */

// --------------------------- 1) 静态配置与常量 ---------------------------
const CACHE = {
    data: {},
    ts: 0,
    ttl: 60000,
    // locks: Map<key, expiresAtMs>
    locks: new Map()
  };
  
  const DEFAULTS = {
    // 基础
    welcome_msg: "欢迎 {name}！请先完成验证。",
  
    // 验证
    enable_verify: "true",
    enable_qa_verify: "true",
    captcha_mode: "turnstile", // turnstile 或 recaptcha
    verif_q: "1+1=?\n提示：答案在简介中。",
    verif_a: "2",
  
    // 风控
    block_threshold: "5",
    enable_admin_receipt: "true",
  
    // 转发开关
    enable_image_forwarding: "true",
    enable_link_forwarding: "true",
    enable_text_forwarding: "true",
    enable_channel_forwarding: "true",
    enable_forward_forwarding: "true",
    enable_audio_forwarding: "true",
    enable_sticker_forwarding: "true",
  
    // 话题与列表
    backup_group_id: "",
    unread_topic_id: "",
    blocked_topic_id: "",
    busy_mode: "false",
    busy_msg: "当前是非营业时间，消息已收到，管理员稍后回复。",
    block_keywords: "[]",
    keyword_responses: "[]",
    authorized_admins: "[]"
  };
  
  // 消息类型定义（转发子开关单独判断，避免 extra 返回 null 造成崩溃）
  const MSG_TYPES = [
    {
      check: m => m.forward_from || m.forward_from_chat,
      key: "enable_forward_forwarding",
      name: "转发消息",
      isChannelForward: m => m.forward_from_chat?.type === "channel"
    },
    { check: m => m.audio || m.voice, key: "enable_audio_forwarding", name: "语音/音频" },
    { check: m => m.sticker || m.animation, key: "enable_sticker_forwarding", name: "贴纸/GIF" },
    { check: m => m.photo || m.video || m.document, key: "enable_image_forwarding", name: "媒体文件" },
    { check: m => (m.entities || []).some(e => ["url", "text_link"].includes(e.type)), key: "enable_link_forwarding", name: "链接" },
    { check: m => m.text, key: "enable_text_forwarding", name: "纯文本" }
  ];
  
  // --------------------------- 2) 过期锁工具（适配 Worker 生命周期） ---------------------------
  function lockHas(key) {
    const now = Date.now();
    const exp = CACHE.locks.get(key);
    if (!exp) return false;
    if (exp <= now) {
      CACHE.locks.delete(key);
      return false;
    }
    return true;
  }
  function lockSet(key, ttlMs) {
    CACHE.locks.set(key, Date.now() + Math.max(1, ttlMs || 1));
  }
  function lockDel(key) {
    CACHE.locks.delete(key);
  }
  
  // --------------------------- 3) Worker 入口（不使用 webhook secret） ---------------------------
  export default {
    async fetch(req, env, ctx) {
      ctx.waitUntil(dbInit(env).catch(e => console.error("DB Init Failed:", e)));
  
      const url = new URL(req.url);
      try {
        if (req.method === "GET") {
          if (url.pathname === "/verify") return handleVerifyPage(url, env);
          if (url.pathname === "/") return new Response("Bot v3.70+ Fusion Hardened (No Webhook Secret + Reaction Receipt)", { status: 200 });
        }
  
        if (req.method === "POST") {
          if (url.pathname === "/submit_token") return handleTokenSubmit(req, env);
  
          try {
            const update = await req.json();
            ctx.waitUntil(handleUpdate(update, env, ctx));
            return new Response("OK");
          } catch {
            return new Response("Bad Request", { status: 400 });
          }
        }
      } catch (e) {
        console.error("Critical Worker Error:", e);
        return new Response("Internal Server Error", { status: 500 });
      }
  
      return new Response("404 Not Found", { status: 404 });
    }
  };
  
  // --------------------------- 4) 数据库封装 ---------------------------
  const safeParse = (str, fb = {}) => {
    try {
      if (typeof str !== "string") return fb;
      return JSON.parse(str);
    } catch {
      return fb;
    }
  };
  
  const sql = async (env, query, args = [], type = "run") => {
    try {
      const stmt = env.TG_BOT_DB.prepare(query).bind(...(Array.isArray(args) ? args : [args]));
      return type === "run" ? await stmt.run() : await stmt[type]();
    } catch (e) {
      console.error(`SQL Fail [${query}]:`, e);
      if (query.match(/^(INSERT|UPDATE|DELETE)/i)) throw e;
      return null;
    }
  };
  
  async function dbInit(env) {
    if (!env.TG_BOT_DB) return;
    await env.TG_BOT_DB.batch([
      env.TG_BOT_DB.prepare(`CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)`),
      env.TG_BOT_DB.prepare(
        `CREATE TABLE IF NOT EXISTS users (
          user_id TEXT PRIMARY KEY,
          user_state TEXT DEFAULT 'new',
          is_blocked INTEGER DEFAULT 0,
          block_count INTEGER DEFAULT 0,
          topic_id TEXT,
          user_info_json TEXT DEFAULT '{}'
        )`
      ),
      env.TG_BOT_DB.prepare(
        `CREATE TABLE IF NOT EXISTS messages (
          user_id TEXT,
          message_id TEXT,
          text TEXT,
          date INTEGER,
          PRIMARY KEY (user_id, message_id)
        )`
      )
    ]);
  }
  
  async function getCfg(k, env) {
    if (typeof k !== "string" || !k) return "";
    const now = Date.now();
    if (CACHE.ts && now - CACHE.ts < CACHE.ttl && CACHE.data[k] !== undefined) return CACHE.data[k];
  
    const rows = await sql(env, "SELECT * FROM config", [], "all");
    if (rows?.results) {
      CACHE.data = {};
      rows.results.forEach(r => (CACHE.data[r.key] = r.value));
      CACHE.ts = now;
    }
  
    const envK = k
      .toUpperCase()
      .replace(/_MSG|_Q|_A/, m => ({ _MSG: "_MESSAGE", _Q: "_QUESTION", _A: "_ANSWER" }[m]));
    return CACHE.data[k] ?? (env[envK] || DEFAULTS[k] || "");
  }
  
  async function setCfg(k, v, env) {
    if (typeof k !== "string" || !k) return;
    await sql(env, "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)", [k, v]);
    CACHE.ts = 0;
  }
  
  // 合并式更新 user_info，降低并发覆盖丢字段风险
  async function mergeUserInfo(id, patch, env) {
    const row = await sql(env, "SELECT user_info_json FROM users WHERE user_id = ?", id, "first");
    const cur = safeParse(row?.user_info_json, {});
    const merged = {
      ...(cur && typeof cur === "object" ? cur : {}),
      ...(patch && typeof patch === "object" ? patch : {})
    };
    return JSON.stringify(merged);
  }
  
  async function getUser(id, env) {
    let u = await sql(env, "SELECT * FROM users WHERE user_id = ?", id, "first");
    if (!u) {
      try {
        await sql(env, "INSERT OR IGNORE INTO users (user_id, user_state, user_info_json) VALUES (?, 'new', ?)", [id, "{}"]);
      } catch {}
      u = await sql(env, "SELECT * FROM users WHERE user_id = ?", id, "first");
    }
  
    if (!u) u = { user_id: id, user_state: "new", is_blocked: 0, block_count: 0, topic_id: null, user_info_json: "{}" };
    u.is_blocked = !!u.is_blocked;
    u.user_info = safeParse(u.user_info_json, {});
    if (!u.user_info || typeof u.user_info !== "object") u.user_info = {};
    return u;
  }
  
  async function updUser(id, data, env) {
    if (data.user_info) {
      data.user_info_json = await mergeUserInfo(id, data.user_info, env);
      delete data.user_info;
    }
  
    const keys = Object.keys(data);
    if (!keys.length) return;
  
    const safeKeys = keys.filter(k => ["user_state", "is_blocked", "block_count", "topic_id", "user_info_json"].includes(k));
    if (!safeKeys.length) return;
  
    const q = `UPDATE users SET ${safeKeys.map(k => `${k}=?`).join(",")} WHERE user_id=?`;
    const v = [...safeKeys.map(k => (typeof data[k] === "boolean" ? (data[k] ? 1 : 0) : data[k])), id];
    try {
      await sql(env, q, v);
    } catch (e) {
      console.error("Update User Failed:", e);
    }
  }
  
  // --------------------------- 5) Telegram API 封装 ---------------------------
  async function api(token, method, body) {
    const r = await fetch(`https://api.telegram.org/bot${token}/${method}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    const d = await r.json();
    if (!d.ok) {
      console.warn(`TG API Error [${method}]:`, d.description);
      throw new Error(d.description);
    }
    return d.result;
  }
  
  // --------------------------- 6) 权限/工具函数 ---------------------------
  function parseIdSet(raw) {
    return new Set(
      (raw || "")
        .split(/[,，\s]+/)
        .map(s => s.trim())
        .filter(Boolean)
    );
  }
  
  const getBool = async (k, e) => (await getCfg(k, e)) === "true";
  const getJsonCfg = async (k, e) => safeParse(await getCfg(k, e), []);
  const escape = t => (t || "").toString().replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
  
  // 正则安全测试：无效正则不崩溃；pattern 过长直接忽略
  function safeRegexTest(pattern, text) {
    if (!pattern || typeof pattern !== "string") return false;
    const p = pattern.trim();
    if (!p) return false;
    if (p.length > 256) return false;
    try {
      return new RegExp(p, "gi").test(text);
    } catch {
      return false;
    }
  }
  
  async function isAuthAdmin(id, e) {
    const idStr = id.toString();
    const adminSet = parseIdSet(e.ADMIN_IDS || "");
    if (adminSet.has(idStr)) return true;
    const extra = await getJsonCfg("authorized_admins", e);
    return Array.isArray(extra) && extra.map(x => x.toString()).includes(idStr);
  }
  
  const getUMeta = (tgUser, dbUser, d) => {
    const id = tgUser.id.toString();
    const name = ((tgUser.first_name || "") + " " + (tgUser.last_name || "")).trim() || "User";
    const timeStr = new Date(d * 1000).toLocaleString("zh-CN", { timeZone: "Asia/Shanghai", hour12: false });
    const note = dbUser.user_info?.note ? `\n📝 <b>备注:</b> ${escape(dbUser.user_info.note)}` : "";
    return {
      userId: id,
      name,
      topicName: `${name} | ${id}`.substring(0, 128),
      card: `<b>🪪 用户资料</b>\n👤: <code>${escape(name)}</code>\n🆔: <code>${id}</code>${note}\n🕒: <code>${timeStr}</code>`
    };
  };
  
  const getBtns = (id, blk) => ({
    inline_keyboard: [
      [{ text: "👤 主页", url: `tg://user?id=${id}` }],
      [{ text: blk ? "✅ 解封" : "🚫 屏蔽", callback_data: `${blk ? "unblock" : "block"}:${id}` }],
      [{ text: "✏️ 备注", callback_data: `note:set:${id}` }, { text: "📌 置顶", callback_data: `pin_card:${id}` }]
    ]
  });
  
  // --------------------------- 7) 核心 update 处理 ---------------------------
  async function handleUpdate(update, env, ctx) {
    try {
      const msg = update.message || update.edited_message;
      if (!msg) return update.callback_query ? handleCallback(update.callback_query, env) : null;
      if (update.edited_message && msg.chat.type === "private") return handleEdit(msg, env);
      if (msg.chat.type === "private") await handlePrivate(msg, env, ctx);
      else if (msg.chat.id.toString() === env.ADMIN_GROUP_ID) await handleAdminReply(msg, env);
    } catch (e) {
      console.error("handleUpdate error:", e);
    }
  }
  
  async function handlePrivate(msg, env, ctx) {
    const id = msg.chat.id.toString();
    const text = msg.text || "";
    const adminSet = parseIdSet(env.ADMIN_IDS || "");
    const isAdm = adminSet.has(id); // 严格匹配
    const isStart = text.startsWith("/start");
  
    // 1) 管理员命令优先
    if (isStart) {
      if (isAdm && ctx) ctx.waitUntil(registerCommands(env));
      if (isAdm) return handleAdminConfig(id, null, "menu", null, null, env);
    }
    if (text === "/help" && isAdm) {
      return api(env.BOT_TOKEN, "sendMessage", {
        chat_id: id,
        text: "ℹ️ <b>帮助</b>\n• 回复消息即对话\n• /start 打开面板",
        parse_mode: "HTML"
      });
    }
  
    // 2) 用户状态
    const u = await getUser(id, env);
  
    // 3) 解封自愈：被封用户发 /start 可重置
    if (u.is_blocked) {
      if (isStart) {
        await updUser(id, { is_blocked: 0, block_count: 0 }, env);
        await manageBlacklist(env, u, msg.from, false);
        return sendStart(id, msg, env);
      }
      return;
    }
  
    // 4) 管理员免验证
    if (await isAuthAdmin(id, env)) {
      if (u.user_state !== "verified") await updUser(id, { user_state: "verified" }, env);
    }
  
    // 5) 管理员输入状态机
    if (isAdm) {
      const stateStr = await getCfg(`admin_state:${id}`, env);
      if (stateStr) {
        const state = safeParse(stateStr, null);
        if (state && state.action === "input") return handleAdminInput(id, msg, state, env);
      }
    }
  
    // 6) 验证拦截
    const verifyOn = await getBool("enable_verify", env);
    const qaOn = await getBool("enable_qa_verify", env);
  
    if (u.user_state !== "verified" && (verifyOn || qaOn)) {
      if (u.user_state === "pending_verification" && text) return verifyAnswer(id, text, env, msg);
      return sendStart(id, msg, env);
    }
  
    // 7) 已验证逻辑
    if (isStart) return sendStart(id, msg, env);
    await handleVerifiedMsg(msg, u, env);
  }
  
  async function sendStart(id, msg, env) {
    const u = await getUser(id, env);
  
    // 已验证且已有话题：/start 仅更新资料卡
    if (u.topic_id && u.user_state === "verified") {
      await sendInfoCardToTopic(env, u, msg.from, u.topic_id);
      await api(env.BOT_TOKEN, "sendMessage", {
        chat_id: id,
        text: "✅ <b>会话已连接</b>\n您可以直接发送消息，管理员会收到。",
        parse_mode: "HTML"
      });
      return;
    }
  
    // 欢迎语（支持媒体 JSON）
    let welcomeRaw = await getCfg("welcome_msg", env);
    const name = escape(msg.from.first_name || "User");
    let media = null,
      txt = welcomeRaw;
  
    try {
      if (welcomeRaw.trim().startsWith("{")) {
        media = safeParse(welcomeRaw, null);
        if (media) txt = media.caption || "";
      }
    } catch {}
  
    txt = txt.replace(/{name}|{user}/g, name);
  
    if (media && media.type) {
      try {
        await api(env.BOT_TOKEN, `send${media.type.charAt(0).toUpperCase() + media.type.slice(1)}`, {
          chat_id: id,
          [media.type]: media.file_id,
          caption: txt,
          parse_mode: "HTML"
        });
      } catch {
        await api(env.BOT_TOKEN, "sendMessage", { chat_id: id, text: txt, parse_mode: "HTML" });
      }
    } else {
      await api(env.BOT_TOKEN, "sendMessage", { chat_id: id, text: txt, parse_mode: "HTML" });
    }
  
    // 验证流程
    const url = (env.WORKER_URL || "").replace(/\/$/, "");
    const vOn = await getBool("enable_verify", env);
    const qaOn = await getBool("enable_qa_verify", env);
  
    if (vOn && url) {
      await updUser(id, { user_state: "pending_turnstile" }, env);
      api(env.BOT_TOKEN, "sendMessage", {
        chat_id: id,
        text: "🛡️ <b>安全验证</b>\n请点击下方按钮完成人机验证以继续。",
        parse_mode: "HTML",
        reply_markup: {
          inline_keyboard: [[{ text: "点击进行验证", web_app: { url: `${url}/verify?user_id=${encodeURIComponent(id)}` } }]]
        }
      }).catch(() => {});
    } else if (qaOn) {
      await updUser(id, { user_state: "pending_verification" }, env);
      api(env.BOT_TOKEN, "sendMessage", {
        chat_id: id,
        text: "❓ <b>安全提问</b>\n" + (await getCfg("verif_q", env)),
        parse_mode: "HTML"
      }).catch(() => {});
    } else {
      await updUser(id, { user_state: "verified" }, env);
    }
  }
  
  // --------------------------- 8) 已验证用户消息处理 ---------------------------
  async function handleVerifiedMsg(msg, u, env) {
    const id = u.user_id;
    const text = msg.text || msg.caption || "";
  
    // A) 屏蔽词检测（防崩溃正则）
    if (text) {
      const kws = await getJsonCfg("block_keywords", env);
      const t = text.slice(0, 2000);
      const hit = (Array.isArray(kws) ? kws : []).some(k => safeRegexTest(k, t));
      if (hit) {
        const c = u.block_count + 1;
        const max = parseInt(await getCfg("block_threshold", env), 10) || 5;
        await updUser(id, { block_count: c, is_blocked: c >= max }, env);
        if (c >= max) {
          await manageBlacklist(env, u, msg.from, true);
          return api(env.BOT_TOKEN, "sendMessage", { chat_id: id, text: "❌ 您已被系统自动封禁" });
        }
        return api(env.BOT_TOKEN, "sendMessage", { chat_id: id, text: `⚠️ 含有违禁词，请勿发送 (${c}/${max})` });
      }
    }
  
    // B) 类型过滤（含频道转发子开关）
    for (const t of MSG_TYPES) {
      if (t.check(msg)) {
        const mainEnabled = await getBool(t.key, env);
        if (!mainEnabled && !(await isAuthAdmin(id, env))) {
          return api(env.BOT_TOKEN, "sendMessage", { chat_id: id, text: `⚠️ 系统不接收 ${t.name}` });
        }
  
        if (t.key === "enable_forward_forwarding" && t.isChannelForward?.(msg)) {
          const chEnabled = await getBool("enable_channel_forwarding", env);
          if (!chEnabled && !(await isAuthAdmin(id, env))) {
            return api(env.BOT_TOKEN, "sendMessage", { chat_id: id, text: `⚠️ 系统不接收 频道转发` });
          }
        }
        break;
      }
    }
  
    // C) 自动回复（防崩溃正则）
    if (text) {
      const rules = await getJsonCfg("keyword_responses", env);
      const t = text.slice(0, 2000);
      const match = (Array.isArray(rules) ? rules : []).find(r => r && safeRegexTest(r.keywords, t));
      if (match) api(env.BOT_TOKEN, "sendMessage", { chat_id: id, text: match.response }).catch(() => {});
    }
  
    // D) 忙碌回复
    if (await getBool("busy_mode", env)) {
      const now = Date.now();
      const last = u.user_info?.last_busy_reply || 0;
      if (now - last > 300000) {
        api(env.BOT_TOKEN, "sendMessage", { chat_id: id, text: "🌙 " + (await getCfg("busy_msg", env)) }).catch(() => {});
        await updUser(id, { user_info: { last_busy_reply: now } }, env);
      }
    }
  
    // E) 转发
    await relayToTopic(msg, u, env);
  }
  
  // --------------------------- 9) ✅ 已送达：Reaction + 兜底文字 ---------------------------
  async function markDelivered(env, chatId, messageId) {
    try {
      // 必须是 ReactionType 数组
      await api(env.BOT_TOKEN, "setMessageReaction", {
        chat_id: chatId,
        message_id: messageId,
        reaction: [{ type: "emoji", emoji: "👍" }],
        is_big: false
      });
    } catch (e) {
      // 兜底：必须带文字，避免 Telegram 把它渲染成巨大 emoji
      api(env.BOT_TOKEN, "sendMessage", {
        chat_id: chatId,
        text: "✅ 已送达",
        reply_to_message_id: messageId,
        disable_notification: true
      }).catch(() => {});
    }
  }
  
  // --------------------------- 10) 转发到话题（Forward -> Copy 降级） ---------------------------
  async function relayToTopic(msg, u, env) {
    const uid = u.user_id;
    const uMeta = getUMeta(msg.from, u, msg.date);
    let tid = u.topic_id;
  
    // 1) 创建话题（过期锁防抖）
    const lockKey = `topic_create:${uid}`;
    if (!tid && lockHas(lockKey)) return;
  
    if (!tid) {
      lockSet(lockKey, 5000);
      try {
        const freshU = await getUser(uid, env);
        if (freshU.topic_id) {
          tid = freshU.topic_id;
        } else {
          const t = await api(env.BOT_TOKEN, "createForumTopic", { chat_id: env.ADMIN_GROUP_ID, name: uMeta.topicName });
          tid = t.message_thread_id.toString();
          await updUser(uid, { topic_id: tid }, env);
          u.topic_id = tid;
  
          // 仅在新建话题时发送资料卡
          await sendInfoCardToTopic(env, u, msg.from, tid);
        }
      } catch (e) {
        console.error("Topic Create Error:", e);
        const existUser = await getUser(uid, env);
        if (existUser.topic_id) tid = existUser.topic_id;
        else return api(env.BOT_TOKEN, "sendMessage", { chat_id: uid, text: "⚠️ 系统繁忙，请稍后重试" });
      } finally {
        lockDel(lockKey);
      }
    }
  
    if (!tid) return;
  
    // 2) 转发（Forward -> Copy）
    let relaySuccess = false;
  
    try {
      await api(env.BOT_TOKEN, "forwardMessage", {
        chat_id: env.ADMIN_GROUP_ID,
        from_chat_id: uid,
        message_id: msg.message_id,
        message_thread_id: tid
      });
      relaySuccess = true;
    } catch (fwErr) {
      try {
        const extra = {};
        if (msg.text) extra.text = msg.text;
        if (msg.caption) extra.caption = msg.caption;
        await api(env.BOT_TOKEN, "copyMessage", {
          chat_id: env.ADMIN_GROUP_ID,
          from_chat_id: uid,
          message_id: msg.message_id,
          message_thread_id: tid,
          ...extra
        });
        relaySuccess = true;
      } catch (cpErr) {
        console.error("Copy Failed:", cpErr);
        if (cpErr.message && (cpErr.message.includes("thread") || cpErr.message.includes("not found"))) {
          await updUser(uid, { topic_id: null }, env);
          return api(env.BOT_TOKEN, "sendMessage", { chat_id: uid, text: "⚠️ 会话已过期，请重发" });
        }
      }
    }
  
    if (relaySuccess) {
      // ✅ 关键：不再发送“大✅”，改为 reaction（失败兜底“✅ 已送达”）
      if (msg?.message_id) markDelivered(env, uid, msg.message_id);
  
      if (msg.text) {
        sql(env, "INSERT OR REPLACE INTO messages (user_id, message_id, text, date) VALUES (?,?,?,?)", [
          uid,
          msg.message_id,
          msg.text,
          msg.date
        ]).catch(() => {});
      }
  
      await Promise.all([handleInbox(env, msg, u, tid, uMeta), handleBackup(msg, uMeta, env)]);
    }
  }
  
  // --------------------------- 11) 资料卡 / 通知 / 备份 / 黑名单 ---------------------------
  async function sendInfoCardToTopic(env, u, tgUser, tid, date) {
    const meta = getUMeta(tgUser, u, date || Date.now() / 1000);
    try {
      const card = await api(env.BOT_TOKEN, "sendMessage", {
        chat_id: env.ADMIN_GROUP_ID,
        message_thread_id: tid,
        text: meta.card,
        parse_mode: "HTML",
        reply_markup: getBtns(u.user_id, u.is_blocked)
      });
  
      await updUser(u.user_id, { user_info: { card_msg_id: card.message_id } }, env);
  
      api(env.BOT_TOKEN, "pinChatMessage", {
        chat_id: env.ADMIN_GROUP_ID,
        message_id: card.message_id,
        message_thread_id: tid
      }).catch(() => {});
  
      return card.message_id;
    } catch {
      return null;
    }
  }
  
  async function handleInbox(env, msg, u, tid, uMeta) {
    const lk = `inbox:${u.user_id}`;
    if (lockHas(lk)) return;
    lockSet(lk, 3000);
  
    let inboxId = await getCfg("unread_topic_id", env);
    if (!inboxId) {
      try {
        const t = await api(env.BOT_TOKEN, "createForumTopic", { chat_id: env.ADMIN_GROUP_ID, name: "🔔 未读消息" });
        inboxId = t.message_thread_id.toString();
        await setCfg("unread_topic_id", inboxId, env);
      } catch {
        return;
      }
    }
  
    const gid = env.ADMIN_GROUP_ID.toString().replace(/^-100/, "");
    const preview = msg.text ? (msg.text.length > 20 ? msg.text.substring(0, 20) + "..." : msg.text) : "[媒体消息]";
    const cardText = `<b>🔔 新消息</b>\n${uMeta.card}\n📝 <b>预览:</b> ${escape(preview)}`;
    const kb = {
      inline_keyboard: [[
        { text: "🚀 直达回复", url: `https://t.me/c/${gid}/${tid}` },
        { text: "✅ 已阅", callback_data: `inbox:del:${u.user_id}` }
      ]]
    };
  
    try {
      if (u.user_info?.inbox_msg_id) {
        try {
          await api(env.BOT_TOKEN, "editMessageText", {
            chat_id: env.ADMIN_GROUP_ID,
            message_id: u.user_info.inbox_msg_id,
            message_thread_id: inboxId,
            text: cardText,
            parse_mode: "HTML",
            reply_markup: kb
          });
          await updUser(u.user_id, { user_info: { last_notify: Date.now() } }, env);
          return;
        } catch {
          // 编辑失败继续发新通知
        }
      }
  
      const nm = await api(env.BOT_TOKEN, "sendMessage", {
        chat_id: env.ADMIN_GROUP_ID,
        message_thread_id: inboxId,
        text: cardText,
        parse_mode: "HTML",
        reply_markup: kb
      });
  
      await updUser(u.user_id, { user_info: { last_notify: Date.now(), inbox_msg_id: nm.message_id } }, env);
    } catch (e) {
      if (e.message && e.message.includes("thread")) await setCfg("unread_topic_id", "", env);
    }
  }
  
  async function handleBackup(msg, meta, env) {
    const bid = await getCfg("backup_group_id", env);
    if (!bid) return;
    try {
      await api(env.BOT_TOKEN, "copyMessage", { chat_id: bid, from_chat_id: msg.chat.id, message_id: msg.message_id });
    } catch {
      if (msg.text) api(env.BOT_TOKEN, "sendMessage", { chat_id: bid, text: `<b>备份</b> ${meta.name}:\n${msg.text}`, parse_mode: "HTML" }).catch(() => {});
    }
  }
  
  async function manageBlacklist(env, u, tgUser, isBlocking) {
    let bid = await getCfg("blocked_topic_id", env);
    if (!bid && isBlocking) {
      try {
        const t = await api(env.BOT_TOKEN, "createForumTopic", { chat_id: env.ADMIN_GROUP_ID, name: "🚫 黑名单" });
        bid = t.message_thread_id.toString();
        await setCfg("blocked_topic_id", bid, env);
      } catch {
        return;
      }
    }
    if (!bid) return;
  
    if (isBlocking) {
      const meta = getUMeta(tgUser, u, Date.now() / 1000);
      const m = await api(env.BOT_TOKEN, "sendMessage", {
        chat_id: env.ADMIN_GROUP_ID,
        message_thread_id: bid,
        text: `<b>🚫 用户已屏蔽</b>\n${meta.card}`,
        parse_mode: "HTML",
        reply_markup: { inline_keyboard: [[{ text: "✅ 解除屏蔽", callback_data: `unblock:${u.user_id}` }]] }
      }).catch(() => {});
  
      if (m) await updUser(u.user_id, { user_info: { blacklist_msg_id: m.message_id } }, env);
    } else {
      const mid = u.user_info?.blacklist_msg_id;
      if (mid) {
        api(env.BOT_TOKEN, "deleteMessage", { chat_id: env.ADMIN_GROUP_ID, message_id: mid }).catch(() => {});
        await updUser(u.user_id, { user_info: { blacklist_msg_id: null } }, env);
      }
    }
  }
  
  // --------------------------- 12) Web 验证页面（Mini App） ---------------------------
  async function handleVerifyPage(url, env) {
    const uid = (url.searchParams.get("user_id") || "").toString();
    const mode = await getCfg("captcha_mode", env);
    const siteKey = mode === "recaptcha" ? env.RECAPTCHA_SITE_KEY : env.TURNSTILE_SITE_KEY;
    if (!siteKey) return new Response("Misconfigured", { status: 400 });
  
    const script = mode === "recaptcha" ? "https://www.google.com/recaptcha/api.js" : "https://challenges.cloudflare.com/turnstile/v0/api.js";
    const divClass = mode === "recaptcha" ? "g-recaptcha" : "cf-turnstile";
  
    const uidJson = JSON.stringify(uid);
  
    const html = `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
  <script src="https://telegram.org/js/telegram-web-app.js"></script>
  <script src="${script}" async defer></script>
  <style>body{display:flex;justify-content:center;align-items:center;height:100vh;background:#fff;font-family:sans-serif}
  #c{text-align:center;padding:20px;background:#f0f0f0;border-radius:10px;max-width:90vw}
  </style></head><body>
  <div id="c"><h3>🛡️ 安全验证</h3><div class="${divClass}" data-sitekey="${siteKey}" data-callback="S"></div><div id="m"></div></div>
  <script>
  const tg=window.Telegram.WebApp; tg.ready();
  const fallbackUserId = ${uidJson};
  function S(t){
    document.getElementById('m').innerText='Wait...';
    const initData = tg.initData || "";
    fetch('/submit_token',{
      method:'POST',
      headers:{'Content-Type':'application/json'},
      body: JSON.stringify({ token:t, userId: fallbackUserId, initData })
    })
    .then(r=>r.json())
    .then(d=>{
      if(d.success){
        document.getElementById('m').innerText='✅';
        setTimeout(()=>{tg.close();try{window.close()}catch(e){}},800);
      }else{
        document.getElementById('m').innerText='❌ ' + (d.error||'');
      }
    })
    .catch(()=>{ document.getElementById('m').innerText='Error'; });
  }
  </script></body></html>`;
  
    return new Response(html, { headers: { "Content-Type": "text/html; charset=utf-8" } });
  }
  
  async function handleTokenSubmit(req, env) {
    try {
      const body = await req.json();
      const token = body?.token;
  
      const mode = await getCfg("captcha_mode", env);
      const verifyUrl = mode === "recaptcha"
        ? "https://www.google.com/recaptcha/api/siteverify"
        : "https://challenges.cloudflare.com/turnstile/v0/siteverify";
  
      const params = mode === "recaptcha"
        ? new URLSearchParams({ secret: env.RECAPTCHA_SECRET_KEY, response: token })
        : JSON.stringify({ secret: env.TURNSTILE_SECRET_KEY, response: token });
  
      const headers = mode === "recaptcha"
        ? { "Content-Type": "application/x-www-form-urlencoded" }
        : { "Content-Type": "application/json" };
  
      const r = await fetch(verifyUrl, { method: "POST", headers, body: params });
      const d = await r.json();
      if (!d.success) throw new Error("Token Invalid");
  
      // Mini App 强绑定：必须有 initData 且验签，通过 initData.user.id 作为最终 userId
      const initData = (body?.initData || "").toString();
      if (!initData) throw new Error("Missing initData");
      const parsed = await verifyTelegramInitData(initData, env.BOT_TOKEN, 600);
      const userId = parsed.userId;
      if (!userId) throw new Error("No user in initData");
  
      const qaOn = await getBool("enable_qa_verify", env);
  
      if (qaOn) {
        await updUser(userId, { user_state: "pending_verification" }, env);
        await api(env.BOT_TOKEN, "sendMessage", {
          chat_id: userId,
          text: "✅ 验证通过！\n请继续回答：\n" + (await getCfg("verif_q", env))
        });
      } else {
        await updUser(userId, { user_state: "verified" }, env);
        await api(env.BOT_TOKEN, "sendMessage", { chat_id: userId, text: "✅ 验证通过！" });
  
        // 验证通过：若无 topic_id，直接建话题并发首卡（避免伪造 message_id=0 的转发）
        const u = await getUser(userId, env);
        if (!u.topic_id) {
          const meta = getUMeta({ id: userId, first_name: "User" }, u, Date.now() / 1000);
          const t = await api(env.BOT_TOKEN, "createForumTopic", { chat_id: env.ADMIN_GROUP_ID, name: meta.topicName });
          const tid = t.message_thread_id.toString();
          await updUser(userId, { topic_id: tid }, env);
          u.topic_id = tid;
          await sendInfoCardToTopic(env, u, { id: userId, first_name: "User" }, tid, Date.now() / 1000);
        }
      }
  
      return new Response(JSON.stringify({ success: true }), { headers: { "Content-Type": "application/json" } });
    } catch (e) {
      return new Response(JSON.stringify({ success: false, error: e?.message || "failed" }), {
        status: 400,
        headers: { "Content-Type": "application/json" }
      });
    }
  }
  
  // --------------------------- 13) initData 验签（官方算法） ---------------------------
  async function verifyTelegramInitData(initData, botToken, maxAgeSec) {
    const params = new URLSearchParams(initData);
    const hash = params.get("hash") || "";
    if (!hash) throw new Error("initData missing hash");
  
    const authDateStr = params.get("auth_date") || "";
    const authDate = parseInt(authDateStr, 10);
    if (!authDate || !Number.isFinite(authDate)) throw new Error("initData missing auth_date");
  
    const nowSec = Math.floor(Date.now() / 1000);
    if (maxAgeSec && nowSec - authDate > maxAgeSec) throw new Error("initData expired");
  
    const pairs = [];
    for (const [k, v] of params.entries()) {
      if (k === "hash") continue;
      pairs.push([k, v]);
    }
    pairs.sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
    const dataCheckString = pairs.map(([k, v]) => `${k}=${v}`).join("\n");
  
    // secret_key = HMAC_SHA256(key="WebAppData", data=bot_token)
    const secretKey = await hmacSha256Bytes(strToBytes("WebAppData"), strToBytes(botToken));
    // calc_hash = hex(HMAC_SHA256(key=secret_key, data=data_check_string))
    const calc = await hmacSha256Bytes(secretKey, strToBytes(dataCheckString));
    const calcHex = bytesToHex(calc);
  
    if (!timingSafeEqualHex(calcHex, hash)) throw new Error("initData hash mismatch");
  
    const userJson = params.get("user");
    let userId = "";
    try {
      if (userJson) {
        const userObj = JSON.parse(userJson);
        if (userObj && (userObj.id || userObj.id === 0)) userId = userObj.id.toString();
      }
    } catch {}
  
    return { userId, authDate };
  }
  
  function strToBytes(s) {
    return new TextEncoder().encode(s);
  }
  async function hmacSha256Bytes(keyBytes, dataBytes) {
    const key = await crypto.subtle.importKey("raw", keyBytes, { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
    const sig = await crypto.subtle.sign("HMAC", key, dataBytes);
    return new Uint8Array(sig);
  }
  function bytesToHex(u8) {
    let out = "";
    for (const b of u8) out += b.toString(16).padStart(2, "0");
    return out;
  }
  function timingSafeEqualHex(a, b) {
    const aa = (a || "").toLowerCase();
    const bb = (b || "").toLowerCase();
    if (aa.length !== bb.length) return false;
    let r = 0;
    for (let i = 0; i < aa.length; i++) r |= aa.charCodeAt(i) ^ bb.charCodeAt(i);
    return r === 0;
  }
  
  // --------------------------- 14) QA 验证 ---------------------------
  async function verifyAnswer(id, ans, env, msg) {
    if (ans.trim() === (await getCfg("verif_a", env)).trim()) {
      await updUser(id, { user_state: "verified" }, env);
      await api(env.BOT_TOKEN, "sendMessage", { chat_id: id, text: "✅ 验证通过！" });
      const u = await getUser(id, env);
      await relayToTopic(msg, u, env);
    } else {
      await api(env.BOT_TOKEN, "sendMessage", { chat_id: id, text: "❌ 错误" });
    }
  }
  
  // --------------------------- 15) Commands 注册 ---------------------------
  async function registerCommands(env) {
    try {
      await api(env.BOT_TOKEN, "deleteMyCommands", { scope: { type: "default" } });
      await api(env.BOT_TOKEN, "setMyCommands", { commands: [{ command: "start", description: "开始 / Start" }], scope: { type: "default" } });
  
      const admins = [...(env.ADMIN_IDS || "").split(/[,，]/), ...(await getJsonCfg("authorized_admins", env))];
      const uniqueAdmins = [...new Set(admins.map(i => i.toString().trim()).filter(Boolean))];
  
      for (const id of uniqueAdmins) {
        await api(env.BOT_TOKEN, "setMyCommands", {
          commands: [
            { command: "start", description: "面板" },
            { command: "help", description: "帮助" }
          ],
          scope: { type: "chat", chat_id: id }
        });
      }
    } catch {}
  }
  
  // --------------------------- 16) 回调处理 ---------------------------
  async function handleCallback(cb, env) {
    const { data, message: msg, from } = cb;
    const parts = (data || "").split(":");
    const act = parts[0] || "";
    const p1 = parts[1] || "";
    const p2 = parts[2] || "";
    const p3 = parts[3] || "";
  
    // 收件箱 - 已阅
    if (act === "inbox" && p1 === "del") {
      await api(env.BOT_TOKEN, "deleteMessage", { chat_id: msg.chat.id, message_id: msg.message_id }).catch(() => {});
      if (p2) await updUser(p2, { user_info: { last_notify: 0 } }, env);
      return api(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: cb.id, text: "已处理" }).catch(() => {});
    }
  
    // 备注设置
    if (act === "note" && p1 === "set") {
      await setCfg(`admin_state:${from.id}`, JSON.stringify({ action: "input_note", target: p2 }), env);
      api(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: cb.id, text: "请输入备注" }).catch(() => {});
      return api(env.BOT_TOKEN, "sendMessage", {
        chat_id: msg.chat.id,
        message_thread_id: msg.message_thread_id,
        text: "⌨️ 请回复备注内容 (回复 /clear 清除):"
      });
    }
  
    // 配置菜单路由
    if (act === "config") {
      const adminSet = parseIdSet(env.ADMIN_IDS || "");
      if (!adminSet.has(from.id.toString())) {
        return api(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: cb.id, text: "无权", show_alert: true }).catch(() => {});
      }
  
      // 轮换验证码模式
      if (p1 === "rotate_mode") {
        const currentMode = await getCfg("captcha_mode", env);
        const isEnabled = await getBool("enable_verify", env);
        let nextMode = "turnstile";
        let nextEnable = "true";
        let toast = "已切换: Cloudflare";
  
        if (isEnabled) {
          if (currentMode === "turnstile") {
            nextMode = "recaptcha";
            toast = "已切换: Google";
          } else {
            nextEnable = "false";
            nextMode = currentMode;
            toast = "验证已关闭";
          }
        }
  
        await setCfg("captcha_mode", nextMode, env);
        await setCfg("enable_verify", nextEnable, env);
        await api(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: cb.id, text: toast }).catch(() => {});
        return handleAdminConfig(msg.chat.id, msg.message_id, "menu", "base", null, env);
      }
  
      await api(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: cb.id }).catch(() => {});
      return handleAdminConfig(msg.chat.id, msg.message_id, p1, p2, p3, env);
    }
  
    // 黑名单/封禁管理
    if (msg.chat.id.toString() === env.ADMIN_GROUP_ID && ["block", "unblock"].includes(act)) {
      const isB = act === "block";
      const uid = p1;
      const u = await getUser(uid, env);
  
      await updUser(uid, { is_blocked: isB, block_count: 0 }, env);
  
      // 更新资料卡按钮
      const cardId = u.user_info?.card_msg_id;
      if (cardId) {
        api(env.BOT_TOKEN, "editMessageReplyMarkup", {
          chat_id: env.ADMIN_GROUP_ID,
          message_id: cardId,
          reply_markup: getBtns(uid, isB)
        }).catch(() => {});
      }
  
      await manageBlacklist(env, u, { id: uid, first_name: "User" }, isB);
      api(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: cb.id, text: isB ? "已屏蔽" : "已解封" }).catch(() => {});
      return;
    }
  
    // 资料卡置顶
    if (act === "pin_card") {
      api(env.BOT_TOKEN, "pinChatMessage", { chat_id: msg.chat.id, message_id: msg.message_id, message_thread_id: msg.message_thread_id }).catch(() => {});
      api(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: cb.id, text: "已置顶" }).catch(() => {});
      return;
    }
  }
  
  // --------------------------- 17) 管理员回复（群内话题 -> 私聊） ---------------------------
  async function handleAdminReply(msg, env) {
    if (!msg.message_thread_id || msg.from.is_bot) return;
    if (!(await isAuthAdmin(msg.from.id, env))) return;
  
    // 备注输入状态
    const stateStr = await getCfg(`admin_state:${msg.from.id}`, env);
    if (stateStr) {
      const state = safeParse(stateStr, null);
      if (state && state.action === "input_note") {
        const note = msg.text === "/clear" || msg.text === "清除" ? "" : (msg.text || "");
        await updUser(state.target, { user_info: { note } }, env);
        await setCfg(`admin_state:${msg.from.id}`, "", env);
  
        const u = await getUser(state.target, env);
        const cardId = u.user_info?.card_msg_id;
        if (u.topic_id && cardId) {
          const meta = getUMeta({ id: state.target, first_name: "User" }, u, u.user_info?.join_date || Date.now() / 1000);
          api(env.BOT_TOKEN, "editMessageText", {
            chat_id: env.ADMIN_GROUP_ID,
            message_id: cardId,
            text: meta.card,
            parse_mode: "HTML",
            reply_markup: getBtns(state.target, u.is_blocked)
          }).catch(() => {});
        }
  
        return api(env.BOT_TOKEN, "sendMessage", { chat_id: msg.chat.id, message_thread_id: msg.message_thread_id, text: "✅ 备注已更新" });
      }
    }
  
    // 按 topic_id 找回 user_id
    const row = await sql(env, "SELECT user_id FROM users WHERE topic_id = ?", msg.message_thread_id.toString(), "first");
    const uid = row?.user_id;
    if (!uid) return;
  
    try {
      await api(env.BOT_TOKEN, "copyMessage", { chat_id: uid, from_chat_id: msg.chat.id, message_id: msg.message_id });
      if (await getBool("enable_admin_receipt", env)) {
        api(env.BOT_TOKEN, "sendMessage", {
          chat_id: msg.chat.id,
          message_thread_id: msg.message_thread_id,
          text: "✅",
          reply_to_message_id: msg.message_id,
          disable_notification: true
        }).catch(() => {});
      }
    } catch {
      api(env.BOT_TOKEN, "sendMessage", {
        chat_id: msg.chat.id,
        message_thread_id: msg.message_thread_id,
        text: "❌ 发送失败 (用户可能已停止Bot)"
      }).catch(() => {});
    }
  }
  
  // --------------------------- 18) 用户编辑消息提示（可选） ---------------------------
  async function handleEdit(msg, env) {
    const u = await getUser(msg.from.id.toString(), env);
    if (u.topic_id) {
      const txt = msg.text || msg.caption || "[非文本]";
      api(env.BOT_TOKEN, "sendMessage", {
        chat_id: env.ADMIN_GROUP_ID,
        message_thread_id: u.topic_id,
        text: `✏️ <b>用户修改了消息:</b>\n${escape(txt)}`,
        parse_mode: "HTML"
      }).catch(() => {});
    }
  }
  
  // --------------------------- 19) 管理面板 ---------------------------
  async function handleAdminConfig(cid, mid, type, key, val, env) {
    const render = (txt, kb) =>
      api(env.BOT_TOKEN, mid ? "editMessageText" : "sendMessage", {
        chat_id: cid,
        message_id: mid,
        text: txt,
        parse_mode: "HTML",
        reply_markup: kb
      });
  
    const back = { text: "🔙 返回", callback_data: "config:menu" };
  
    try {
      // 主菜单
      if (!type || type === "menu") {
        if (!key) {
          return render("⚙️ <b>控制面板</b>", {
            inline_keyboard: [
              [{ text: "📝 基础", callback_data: "config:menu:base" }, { text: "🤖 自动回复", callback_data: "config:menu:ar" }],
              [{ text: "🚫 屏蔽词", callback_data: "config:menu:kw" }, { text: "🛠 过滤", callback_data: "config:menu:fl" }],
              [{ text: "👮 协管", callback_data: "config:menu:auth" }, { text: "💾 备份/通知", callback_data: "config:menu:bak" }],
              [{ text: "🌙 营业状态", callback_data: "config:menu:busy" }]
            ]
          });
        }
  
        if (key === "base") {
          const mode = await getCfg("captcha_mode", env);
          const captchaOn = await getBool("enable_verify", env);
          const qaOn = await getBool("enable_qa_verify", env);
          let statusText = "❌ 已关闭";
          if (captchaOn) statusText = mode === "recaptcha" ? "Google" : "Cloudflare";
  
          return render(`基础配置\n验证码模式: ${statusText}\n问题验证: ${qaOn ? "✅" : "❌"}`, {
            inline_keyboard: [
              [{ text: "欢迎语", callback_data: "config:edit:welcome_msg" }, { text: "问题", callback_data: "config:edit:verif_q" }, { text: "答案", callback_data: "config:edit:verif_a" }],
              [{ text: `验证码模式: ${statusText} (点击切换)`, callback_data: "config:rotate_mode" }],
              [{ text: `问题验证: ${qaOn ? "✅ 开启" : "❌ 关闭"}`, callback_data: `config:toggle:enable_qa_verify:${!qaOn}` }],
              [back]
            ]
          });
        }
  
        if (key === "fl") return render("🛠 <b>过滤设置</b> (点击切换)", await getFilterKB(env));
        if (["ar", "kw", "auth"].includes(key)) return render(`列表: ${key}`, await getListKB(key, env));
  
        if (key === "bak") {
          const bid = await getCfg("backup_group_id", env);
          const uid = await getCfg("unread_topic_id", env);
          const blk = await getCfg("blocked_topic_id", env);
  
          return render(`💾 <b>备份与通知</b>\n备份群: ${bid || "无"}\n未读话题: ${uid ? `✅ (${uid})` : "⏳"}\n黑名单话题: ${blk ? `✅ (${blk})` : "⏳"}`, {
            inline_keyboard: [
              [{ text: "设备份群", callback_data: "config:edit:backup_group_id" }, { text: "清备份", callback_data: "config:cl:backup_group_id" }],
              [{ text: "重置聚合话题", callback_data: "config:cl:unread_topic_id" }, { text: "重置黑名单", callback_data: "config:cl:blocked_topic_id" }],
              [back]
            ]
          });
        }
  
        if (key === "busy") {
          const on = await getBool("busy_mode", env);
          const bmsg = await getCfg("busy_msg", env);
          return render(`🌙 <b>营业状态</b>\n当前: ${on ? "🔴 休息中" : "🟢 营业中"}\n回复语: ${escape(bmsg)}`, {
            inline_keyboard: [
              [{ text: `切换为 ${on ? "🟢 营业" : "🔴 休息"}`, callback_data: `config:toggle:busy_mode:${!on}` }],
              [{ text: "✏️ 修改回复语", callback_data: "config:edit:busy_msg" }],
              [back]
            ]
          });
        }
      }
  
      // 开关切换
      if (type === "toggle") {
        await setCfg(key, val, env);
        if (key === "busy_mode") return handleAdminConfig(cid, mid, "menu", "busy", null, env);
        if (key === "enable_qa_verify") return handleAdminConfig(cid, mid, "menu", "base", null, env);
        return render("🛠 <b>过滤设置</b>", await getFilterKB(env));
      }
  
      // 清理
      if (type === "cl") {
        await setCfg(key, key === "authorized_admins" ? "[]" : "", env);
        const next =
          key === "unread_topic_id" || key === "blocked_topic_id"
            ? "bak"
            : key === "authorized_admins"
              ? "auth"
              : "bak";
        return handleAdminConfig(cid, mid, "menu", next, null, env);
      }
  
      // 删除列表项
      if (type === "del") {
        const realK = key === "kw" ? "block_keywords" : key === "auth" ? "authorized_admins" : "keyword_responses";
        let l = await getJsonCfg(realK, env);
        l = (Array.isArray(l) ? l : []).filter(i => (i.id || i).toString() !== val);
        await setCfg(realK, JSON.stringify(l), env);
        return render(`列表: ${key}`, await getListKB(key, env));
      }
  
      // 编辑/添加
      if (type === "edit" || type === "add") {
        await setCfg(`admin_state:${cid}`, JSON.stringify({ action: "input", key: key + (type === "add" ? "_add" : "") }), env);
  
        let promptText = `请输入 ${key} 的值 (/cancel 取消):`;
        if (key === "ar" && type === "add") {
          promptText = `请输入自动回复规则，格式：\n<b>关键词===回复内容</b>\n\n例如：价格===请联系人工客服\n(/cancel 取消)`;
        }
        if (key === "welcome_msg") {
          promptText = `请发送新的欢迎语 (/cancel 取消):\n\n• 支持 <b>文字</b> 或 <b>图片/视频/GIF</b>\n• 支持占位符: {name}\n• 直接发送媒体即可`;
        }
  
        return api(env.BOT_TOKEN, "editMessageText", { chat_id: cid, message_id: mid, text: promptText, parse_mode: "HTML" });
      }
    } catch (e) {
      console.error("handleAdminConfig error:", e);
    }
  }
  
  async function getFilterKB(env) {
    const s = async k => ((await getBool(k, env)) ? "✅" : "❌");
    const b = (t, k, v) => ({ text: `${t} ${v}`, callback_data: `config:toggle:${k}:${v === "❌"}` });
  
    const keys = [
      "enable_admin_receipt",
      "enable_forward_forwarding",
      "enable_image_forwarding",
      "enable_audio_forwarding",
      "enable_sticker_forwarding",
      "enable_link_forwarding",
      "enable_channel_forwarding",
      "enable_text_forwarding"
    ];
    const vals = await Promise.all(keys.map(k => s(k)));
  
    return {
      inline_keyboard: [
        [b("回执", keys[0], vals[0]), b("转发", keys[1], vals[1])],
        [b("媒体", keys[2], vals[2]), b("语音", keys[3], vals[3])],
        [b("贴纸", keys[4], vals[4]), b("链接", keys[5], vals[5])],
        [b("频道", keys[6], vals[6]), b("文本", keys[7], vals[7])],
        [{ text: "🔙 返回", callback_data: "config:menu" }]
      ]
    };
  }
  
  async function getListKB(type, env) {
    const k = type === "ar" ? "keyword_responses" : type === "kw" ? "block_keywords" : "authorized_admins";
    const l = await getJsonCfg(k, env);
    const arr = Array.isArray(l) ? l : [];
  
    const btns = arr.map(i => [
      { text: `🗑 ${type === "ar" ? i.keywords : i}`, callback_data: `config:del:${type}:${i.id || i}` }
    ]);
  
    btns.push([{ text: "➕ 添加", callback_data: `config:add:${type}` }], [{ text: "🔙 返回", callback_data: "config:menu" }]);
    return { inline_keyboard: btns };
  }
  
  async function handleAdminInput(id, msg, state, env) {
    const txt = msg.text || "";
    if (txt === "/cancel") {
      await sql(env, "DELETE FROM config WHERE key=?", `admin_state:${id}`);
      return handleAdminConfig(id, null, "menu", null, null, env);
    }
  
    let k = state.key;
    let val = txt;
  
    try {
      if (k === "welcome_msg") {
        if (msg.photo || msg.video || msg.animation) {
          let fileId, type;
          if (msg.photo) {
            type = "photo";
            fileId = msg.photo[msg.photo.length - 1].file_id;
          } else if (msg.video) {
            type = "video";
            fileId = msg.video.file_id;
          } else if (msg.animation) {
            type = "animation";
            fileId = msg.animation.file_id;
          }
          val = JSON.stringify({ type, file_id: fileId, caption: msg.caption || "" });
        } else {
          val = txt;
        }
      } else if (k.endsWith("_add")) {
        k = k.replace("_add", "");
        const realK = k === "ar" ? "keyword_responses" : k === "kw" ? "block_keywords" : "authorized_admins";
  
        const list = await getJsonCfg(realK, env);
        const arr = Array.isArray(list) ? list : [];
  
        if (k === "ar") {
          const [kk, rr] = txt.split("===");
          if (kk && rr) arr.push({ keywords: kk, response: rr, id: Date.now() });
          else return api(env.BOT_TOKEN, "sendMessage", { chat_id: id, text: "❌ 格式错误，请使用：关键词===回复内容" });
        } else {
          arr.push(txt);
        }
  
        val = JSON.stringify(arr);
        k = realK;
      } else if (k === "authorized_admins") {
        val = JSON.stringify(txt.split(/[,，]/).map(s => s.trim()).filter(Boolean));
      }
  
      await setCfg(k, val, env);
      await sql(env, "DELETE FROM config WHERE key=?", `admin_state:${id}`);
  
      const displayVal = val.startsWith("{") && k === "welcome_msg" ? "[媒体配置]" : val.substring(0, 100);
      await api(env.BOT_TOKEN, "sendMessage", { chat_id: id, text: `✅ ${k} 已更新:\n${displayVal}` }).catch(() => {});
      await handleAdminConfig(id, null, "menu", null, null, env);
    } catch (e) {
      api(env.BOT_TOKEN, "sendMessage", { chat_id: id, text: `❌ 失败: ${e.message}` }).catch(() => {});
    }
  }
  
