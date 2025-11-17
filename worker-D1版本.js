/**
 * Telegram 双向机器人 Cloudflare Worker (D1 版本 - Web App 优化版)
 * * [集成 Cloudflare Turnstile]
 * - 使用 Telegram Mini App (Web App) 进行人机验证，无需跳转外部浏览器。
 * - 验证成功后自动关闭窗口。
 * * * [部署要求]
 * 1. 绑定 D1 数据库为 TG_BOT_DB
 * 2. 环境变量:
 * - WORKER_URL: Worker 的完整 URL (例如 https://my-worker.example.workers.dev)
 * - TURNSTILE_SITE_KEY: Cloudflare Turnstile 站点密钥
 * - TURNSTILE_SECRET_KEY: Cloudflare Turnstile 密钥
 * - BOT_TOKEN: Telegram Bot Token
 * - ADMIN_GROUP_ID: 管理员群组 ID
 * - ADMIN_IDS: 主管理员 ID (逗号分隔)
 */


// --- 辅助函数 (D1 数据库抽象层) ---

/**
 * [D1 Abstraction] 获取全局配置 (config table)
 */
async function dbConfigGet(key, env) {
    const row = await env.TG_BOT_DB.prepare("SELECT value FROM config WHERE key = ?").bind(key).first();
    return row ? row.value : null;
}

/**
 * [D1 Abstraction] 设置/更新全局配置 (config table)
 */
async function dbConfigPut(key, value, env) {
    // INSERT OR REPLACE 确保如果键已存在则更新，否则插入
    await env.TG_BOT_DB.prepare("INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)").bind(key, value).run();
}

/**
 * [D1 Abstraction] 确保用户在 users 表中存在，并返回其数据。
 * 如果用户不存在，则创建默认记录。
 */
async function dbUserGetOrCreate(userId, env) {
    let user = await env.TG_BOT_DB.prepare("SELECT * FROM users WHERE user_id = ?").bind(userId).first();

    if (!user) {
        // 插入默认记录
        await env.TG_BOT_DB.prepare(
            "INSERT INTO users (user_id, user_state, is_blocked, block_count, first_message_sent) VALUES (?, 'new', 0, 0, 0)"
        ).bind(userId).run();
        // 重新查询以获取完整的默认记录
        user = await env.TG_BOT_DB.prepare("SELECT * FROM users WHERE user_id = ?").bind(userId).first();
    }
    
    // 将 is_blocked 转换为布尔值，并解析 JSON 字段
    if (user) {
        user.is_blocked = user.is_blocked === 1;
        user.first_message_sent = user.first_message_sent === 1;
        user.user_info = user.user_info_json ? JSON.parse(user.user_info_json) : null;
    }
    return user;
}

/**
 * [D1 Abstraction] 更新 users 表中的一个或多个字段
 * data 应该是一个包含要更新字段的对象 { topic_id: '...', user_state: '...' }
 */
async function dbUserUpdate(userId, data, env) {
    // 确保 user_info_json 是 JSON 字符串
    if (data.user_info) {
        data.user_info_json = JSON.stringify(data.user_info);
        delete data.user_info; // 移除原始对象以避免与 SQL 冲突
    }
    
    // 构造 SQL 语句
    const fields = Object.keys(data).map(key => {
        // 特殊处理布尔值
        if ((key === 'is_blocked' || key === 'first_message_sent') && typeof data[key] === 'boolean') {
             return `${key} = ?`; // D1 存储 0/1
        }
        return `${key} = ?`;
    }).join(', ');
    
    // 构造值数组
    const values = Object.keys(data).map(key => {
         if ((key === 'is_blocked' || key === 'first_message_sent') && typeof data[key] === 'boolean') {
             return data[key] ? 1 : 0;
         }
         return data[key];
    });
    
    await env.TG_BOT_DB.prepare(`UPDATE users SET ${fields} WHERE user_id = ?`).bind(...values, userId).run();
}

/**
 * [D1 Abstraction] 根据 topic_id 查找 user_id
 */
async function dbTopicUserGet(topicId, env) {
    const row = await env.TG_BOT_DB.prepare("SELECT user_id FROM users WHERE topic_id = ?").bind(topicId).first();
    return row ? row.user_id : null;
}

/**
 * [D1 Abstraction] 存入消息数据 (messages table)
 * 用于已编辑消息跟踪。
 */
async function dbMessageDataPut(userId, messageId, data, env) {
    // data 包含 { text, date }
    await env.TG_BOT_DB.prepare(
        "INSERT OR REPLACE INTO messages (user_id, message_id, text, date) VALUES (?, ?, ?, ?)"
    ).bind(userId, messageId, data.text, data.date).run();
}

/**
 * [D1 Abstraction] 获取消息数据 (messages table)
 * 用于已编辑消息跟踪。
 */
async function dbMessageDataGet(userId, messageId, env) {
    const row = await env.TG_BOT_DB.prepare(
        "SELECT text, date FROM messages WHERE user_id = ? AND message_id = ?"
    ).bind(userId, messageId).first();
    return row || null;
}


/**
 * [D1 Abstraction] 清除管理员编辑状态
 */
async function dbAdminStateDelete(userId, env) {
    await env.TG_BOT_DB.prepare("DELETE FROM config WHERE key = ?").bind(`admin_state:${userId}`).run();
}

/**
 * [D1 Abstraction] 获取管理员编辑状态
 */
async function dbAdminStateGet(userId, env) {
    const stateJson = await dbConfigGet(`admin_state:${userId}`, env);
    return stateJson || null;
}

/**
 * [D1 Abstraction] 设置管理员编辑状态
 */
async function dbAdminStatePut(userId, stateJson, env) {
    await dbConfigPut(`admin_state:${userId}`, stateJson, env);
}

/**
 * [D1 Abstraction] D1 数据库迁移/初始化函数
 * 确保所需的表存在。
 */
async function dbMigrate(env) {
    // 确保 D1 绑定存在
    if (!env.TG_BOT_DB) {
        throw new Error("D1 database binding 'TG_BOT_DB' is missing.");
    }
    
    // config 表
    const configTableQuery = `
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        );
    `;

    // users 表 (存储用户状态、话题ID、屏蔽状态和用户信息)
    const usersTableQuery = `
        CREATE TABLE IF NOT EXISTS users (
            user_id TEXT PRIMARY KEY NOT NULL,
            user_state TEXT NOT NULL DEFAULT 'new',
            is_blocked INTEGER NOT NULL DEFAULT 0,
            block_count INTEGER NOT NULL DEFAULT 0,
            first_message_sent INTEGER NOT NULL DEFAULT 0,
            topic_id TEXT,
            user_info_json TEXT 
        );
    `;
    
    // messages 表 (存储消息内容用于处理已编辑消息)
    const messagesTableQuery = `
        CREATE TABLE IF NOT EXISTS messages (
            user_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            text TEXT,
            date INTEGER,
            PRIMARY KEY (user_id, message_id)
        );
    `;

    // 按批次执行所有创建表的语句
    try {
        await env.TG_BOT_DB.batch([
            env.TG_BOT_DB.prepare(configTableQuery),
            env.TG_BOT_DB.prepare(usersTableQuery),
            env.TG_BOT_DB.prepare(messagesTableQuery),
        ]);
    } catch (e) {
        console.error("D1 Migration Failed:", e);
        throw new Error(`D1 Initialization Failed: ${e.message}`);
    }
}


// --- 辅助函数 ---

function escapeHtml(text) {
  if (!text) return '';
  return text.toString()
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
}

function getUserInfo(user, initialTimestamp = null) {
    const userId = user.id.toString();
    const rawName = (user.first_name || "") + (user.last_name ? ` ${user.last_name}` : "");
    const rawUsername = user.username ? `@${user.username}` : "无";
    
    const safeName = escapeHtml(rawName);
    const safeUsername = escapeHtml(rawUsername);
    const safeUserId = escapeHtml(userId);

    const topicName = `${rawName.trim()} | ${userId}`.substring(0, 128);

    const timestamp = initialTimestamp ? new Date(initialTimestamp * 1000).toLocaleString('zh-CN') : new Date().toLocaleString('zh-CN');
    
    const usernameDisplay = rawUsername !== '无' 
        ? `<a href="tg://user?id=${userId}">${safeUsername}</a>` 
        : `<code>${safeUsername}</code>`;

    const infoCard = `
<b>👤 用户资料卡</b>
---
• 昵称/名称: <code>${safeName}</code>
• 用户名: ${usernameDisplay}
• ID: <code>${safeUserId}</code>
• 首次连接时间: <code>${timestamp}</code>
    `.trim();

    return { userId, name: rawName, username: rawUsername, topicName, infoCard };
}

function getInfoCardButtons(userId, isBlocked) {
    const blockAction = isBlocked ? "unblock" : "block";
    const blockText = isBlocked ? "✅ 解除屏蔽 (Unblock)" : "🚫 屏蔽此人 (Block)";
    return {
        inline_keyboard: [
            [{
                text: blockText,
                callback_data: `${blockAction}:${userId}`
            }],
            [{
                text: "📌 置顶此消息 (Pin Card)",
                callback_data: `pin_card:${userId}` 
            }]
        ]
    };
}


async function getConfig(key, env, defaultValue) {
    const configValue = await dbConfigGet(key, env);
    if (configValue !== null) {
        return configValue;
    }
    const envKey = key.toUpperCase()
                      .replace('WELCOME_MSG', 'WELCOME_MESSAGE')
                      .replace('VERIF_Q', 'VERIFICATION_QUESTION')
                      .replace('VERIF_A', 'VERIFICATION_ANSWER')
                      .replace(/_FORWARDING/g, '_FORWARDING');
    
    const envValue = env[envKey];
    if (envValue !== undefined && envValue !== null) {
        return envValue;
    }
    return defaultValue;
}

function isPrimaryAdmin(userId, env) {
    if (!env.ADMIN_IDS) return false;
    const adminIds = env.ADMIN_IDS.split(',').map(id => id.trim());
    return adminIds.includes(userId.toString());
}


async function getAuthorizedAdmins(env) {
    const jsonString = await getConfig('authorized_admins', env, '[]');
    try {
        const adminList = JSON.parse(jsonString);
        return Array.isArray(adminList) ? adminList.map(id => id.toString().trim()).filter(id => id !== "") : [];
    } catch (e) {
        console.error("Failed to parse authorized_admins from D1:", e);
        return [];
    }
}

async function isAdminUser(userId, env) {
    if (isPrimaryAdmin(userId, env)) {
        return true;
    }
    const authorizedAdmins = await getAuthorizedAdmins(env);
    return authorizedAdmins.includes(userId.toString());
}


// --- 规则管理重构区域 ---

async function getAutoReplyRules(env) {
    const jsonString = await getConfig('keyword_responses', env, '[]');
    try {
        const rules = JSON.parse(jsonString);
        return Array.isArray(rules) ? rules : [];
    } catch (e) {
        console.error("Failed to parse keyword_responses from D1:", e);
        return [];
    }
}

async function getBlockKeywords(env) {
    const jsonString = await getConfig('block_keywords', env, '[]');
    try {
        const keywords = JSON.parse(jsonString);
        return Array.isArray(keywords) ? keywords : [];
    } catch (e) {
        console.error("Failed to parse block_keywords from D1:", e);
        return [];
    }
}


// --- API 客户端 ---

async function telegramApi(token, methodName, params = {}) {
    const url = `https://api.telegram.org/bot${token}/${methodName}`;
    const response = await fetch(url, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify(params),
    });

    let data;
    try {
        data = await response.json();
    } catch (e) {
        console.error(`Telegram API ${methodName} 返回非 JSON 响应`);
        throw new Error(`Telegram API ${methodName} returned non-JSON response`);
    }

    if (!data.ok) {
        throw new Error(`${methodName} failed: ${data.description || JSON.stringify(data)}`);
    }

    return data.result;
}


// --- Cloudflare Turnstile 验证辅助函数 (Web App 优化版) ---

async function validateTurnstile(token, env) {
    if (!token) return false;
    if (!env.TURNSTILE_SECRET_KEY) {
        console.error("Turnstile validation failed: TURNSTILE_SECRET_KEY is not set.");
        return false;
    }

    try {
        const response = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                secret: env.TURNSTILE_SECRET_KEY,
                response: token,
            }),
        });

        const data = await response.json();
        return data.success === true;
    } catch (e) {
        console.error("Error validating Turnstile token:", e.message);
        return false;
    }
}

/**
 * [修改] 处理对 /verify 路径的 GET 请求，返回 Turnstile 验证网页 (适配 Telegram Web App)
 */
async function handleVerificationPage(request, env) {
    const url = new URL(request.url);
    const userId = url.searchParams.get('user_id');

    if (!userId) {
        return new Response("Missing user_id parameter.", { status: 400 });
    }

    if (!env.TURNSTILE_SITE_KEY) {
        return new Response("Bot configuration error (missing site key).", { status: 500 });
    }

    const html = `
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        <title>人机验证</title>
        <script src="https://telegram.org/js/telegram-web-app.js"></script>
        <script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script>
        <style>
            :root {
                --tg-theme-bg-color: #ffffff;
                --tg-theme-text-color: #222222;
                --tg-theme-secondary-bg-color: #f0f0f0;
            }
            body { 
                display: flex; 
                flex-direction: column; 
                justify-content: center; 
                align-items: center; 
                height: 100vh; 
                margin: 0;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                background-color: var(--tg-theme-bg-color); 
                color: var(--tg-theme-text-color); 
                transition: color .2s ease, background-color .2s ease;
            }
            #container { 
                background: var(--tg-theme-secondary-bg-color); 
                padding: 2rem; 
                border-radius: 12px; 
                text-align: center; 
                width: 85%;
                max-width: 350px;
            }
            h2 { margin-top: 0; margin-bottom: 1rem; font-size: 1.5rem; }
            p { margin-bottom: 1.5rem; font-size: 0.95rem; opacity: 0.8; }
            #message { margin-top: 1.5rem; font-size: 1rem; font-weight: bold; min-height: 1.5em; }
            #message.success { color: #2ea043; }
            #message.error { color: #da3633; }
            .cf-turnstile { margin: 0 auto; display: inline-block; }
        </style>
    </head>
    <body>
        <div id="container">
            <h2>🛡️ 安全验证</h2>
            <p>为了防止垃圾信息，请完成下方验证。</p>
            
            <div class="cf-turnstile" 
                 data-sitekey="${env.TURNSTILE_SITE_KEY}" 
                 data-callback="onTurnstileSuccess"
                 data-expired-callback="onTurnstileExpired"
                 data-error-callback="onTurnstileError">
            </div>

            <div id="message"></div>
        </div>

        <script>
            // 初始化 Telegram Web App
            const tg = window.Telegram.WebApp;
            tg.ready();
            try { tg.expand(); } catch(e) {} // 尝试展开视图

            const userId = "${userId}";
            const messageEl = document.getElementById('message');

            function onTurnstileSuccess(token) {
                messageEl.textContent = '验证成功，正在提交...';
                messageEl.className = '';

                fetch('/submit_token', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ token: token, userId: userId })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        messageEl.textContent = '✅ 验证通过！窗口即将关闭...';
                        messageEl.className = 'success';
                        
                        // 验证成功后，通知 Telegram 关闭 Web App 窗口
                        setTimeout(() => {
                            tg.close();
                        }, 1000);
                    } else {
                        messageEl.textContent = '❌ 验证失败，请重试。';
                        messageEl.className = 'error';
                    }
                })
                .catch(err => {
                    console.error('Submit error:', err);
                    messageEl.textContent = '❌ 网络错误。';
                    messageEl.className = 'error';
                });
            }
            
            function onTurnstileExpired() {
                messageEl.textContent = '验证已过期，请重试。';
                messageEl.className = 'error';
            }
            
            function onTurnstileError() {
                 messageEl.textContent = '验证加载失败，请刷新。';
                 messageEl.className = 'error';
            }
        </script>
    </body>
    </html>
    `;

    return new Response(html, {
        headers: { "Content-Type": "text/html; charset=utf-8" },
    });
}

async function handleSubmitToken(request, env) {
    try {
        const { token, userId } = await request.json();

        if (!token || !userId) {
            return new Response(JSON.stringify({ success: false, error: "Missing token or userId" }), {
                status: 400,
                headers: { 'Content-Type': 'application/json' }
            });
        }

        // 1. 验证 Turnstile 令牌
        const isValid = await validateTurnstile(token, env);

        if (!isValid) {
            return new Response(JSON.stringify({ success: false, error: "Invalid Turnstile token" }), {
                status: 403,
                headers: { 'Content-Type': 'application/json' }
            });
        }

        // 2. 验证通过，更新 D1 状态
        await dbUserUpdate(userId, { user_state: "pending_verification" }, env);

        // 3. [关键] 主动向用户发送 L2 验证问题
        const defaultVerifQ = "问题：1+1=?\n\n提示：\n1. 正确答案不是“2”。\n2. 答案在机器人简介内，请看简介的答案进行回答。";
        const verificationQuestion = await getConfig('verif_q', env, defaultVerifQ);
        
        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: userId,
            text: "✅ Cloudflare 验证通过！\n\n现在请回答第二道防线问题（在简介中找到答案）："
        });
        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: userId,
            text: verificationQuestion
        });

        return new Response(JSON.stringify({ success: true }), {
            headers: { 'Content-Type': 'application/json' }
        });

    } catch (e) {
        console.error("handleSubmitToken error:", e.message);
        return new Response(JSON.stringify({ success: false, error: e.message }), {
            status: 500,
            headers: { 'Content-Type': 'application/json' }
        });
    }
}


// --- 核心更新处理函数 ---

export default {
  async fetch(request, env, ctx) {
      // 1. 运行 D1 迁移
      try {
            await dbMigrate(env);
      } catch (e) {
            return new Response(`D1 Database Initialization Error: ${e.message}`, { status: 500 });
      }

      // 2. 检查 Turnstile 环境变量
      if (!env.TURNSTILE_SECRET_KEY || !env.TURNSTILE_SITE_KEY || !env.WORKER_URL) {
          console.error("CRITICAL: Missing TURNSTILE_SECRET_KEY, TURNSTILE_SITE_KEY, or WORKER_URL environment variables.");
      }
      
      const url = new URL(request.url);

      // 3. 路由
      try {
          if (request.method === "GET" && url.pathname === "/verify") {
              return handleVerificationPage(request, env);
          }
          
          if (request.method === "POST" && url.pathname === "/submit_token") {
              return handleSubmitToken(request, env);
          }

          if (request.method === "POST") {
              try {
                  const update = await request.json();
                  ctx.waitUntil(handleUpdate(update, env));
                  return new Response("OK"); 
              } catch (e) {
                  console.error("Failed to parse Telegram update:", e);
                  return new Response("Invalid JSON", { status: 400 });
              }
          }

          if (request.method === "GET" && url.pathname === "/") {
               return new Response("Telegram Bot Worker is running. Use /verify for Turnstile verification.", { status: 200 });
          }

          return new Response("Not found.", { status: 404 });

      } catch (e) {
          console.error("Fetch handler error:", e);
          return new Response("Internal Server Error", { status: 500 });
      }
  },
};

async function handleUpdate(update, env) {
    if (update.message) {
        if (update.message.chat.type === "private") {
            await handlePrivateMessage(update.message, env);
        }
        else if (update.message.chat.id.toString() === env.ADMIN_GROUP_ID) {
            await handleAdminReply(update.message, env);
        }
    } else if (update.edited_message) {
        if (update.edited_message.chat.type === "private") {
            await handleRelayEditedMessage(update.edited_message, env);
        }
    } else if (update.callback_query) {
        await handleCallbackQuery(update.callback_query, env);
    } 
}

async function handlePrivateMessage(message, env) {
    const chatId = message.chat.id.toString();
    const text = message.text || "";
    const userId = chatId;

    const isPrimary = isPrimaryAdmin(userId, env);
    const isAdmin = await isAdminUser(userId, env);
    
    if (text === "/start" || text === "/help") {
        if (isPrimary) { 
            await handleAdminConfigStart(chatId, env);
        } else {
            await handleStart(chatId, env);
        }
        return;
    }
    
    const user = await dbUserGetOrCreate(userId, env);
    const isBlocked = user.is_blocked;

    if (isBlocked) {
        return; 
    }
    
    if (isPrimary) {
        const adminStateJson = await dbAdminStateGet(userId, env);
        if (adminStateJson) {
            await handleAdminConfigInput(userId, text, adminStateJson, env);
            return;
        }
        
        if (user.user_state !== "verified") {
            user.user_state = "verified"; 
            await dbUserUpdate(userId, { user_state: "verified" }, env); 
        }
    }
    
    if (isAdmin && user.user_state !== "verified") {
        user.user_state = "verified"; 
        await dbUserUpdate(userId, { user_state: "verified" }, env); 
    }

    const userState = user.user_state;

    if (userState === "new" || userState === "pending_turnstile") {
        // [修改] 提示重新验证时，使用 web_app 按钮
        if (userState === "pending_turnstile" && env.WORKER_URL) {
            const workerUrl = env.WORKER_URL.replace(/\/$/, '');
            const verificationUrl = `${workerUrl}/verify?user_id=${chatId}`;
            
            const keyboard = { 
                inline_keyboard: [[
                    { 
                        text: "🛡️ 点击进行人机验证", 
                        web_app: { url: verificationUrl } 
                    }
                ]] 
            };
            
            await telegramApi(env.BOT_TOKEN, "sendMessage", { 
                chat_id: chatId, 
                text: "请先点击下方按钮完成第一道 Cloudflare 人机验证。", 
                reply_markup: keyboard 
            });
        } else {
             await telegramApi(env.BOT_TOKEN, "sendMessage", { 
                 chat_id: chatId, 
                 text: "请使用 /start 命令开始验证流程。" 
             });
        }
        return; 

    } else if (userState === "pending_verification") {
        await handleVerification(chatId, text, env);
        return; 
    
    } else if (userState === "verified") {
        
        if (!user.first_message_sent) { 
            const isPureText = message.text &&
                               !message.photo && !message.video && !message.document &&
                               !message.sticker && !message.audio && !message.voice &&
                               !message.forward_from_chat && !message.forward_from && !message.animation &&
                               (!message.entities || message.entities.length === 0);

            if (!isPureText) {
                await telegramApi(env.BOT_TOKEN, "sendMessage", {
                    chat_id: chatId,
                    text: "⚠️ 验证通过后，您的第一条消息必须是纯文本内容（不能包含链接、加粗等格式）。请重新发送。",
                });
                return; 
            }
        }

        // --- [关键词屏蔽检查] ---
        const blockKeywords = await getBlockKeywords(env); 
        const blockThreshold = parseInt(await getConfig('block_threshold', env, "5"), 10) || 5; 
        
        if (blockKeywords.length > 0 && text) { 
            let currentCount = user.block_count;
            
            for (const keyword of blockKeywords) {
                try {
                    const regex = new RegExp(keyword, 'gi'); 
                    if (regex.test(text)) {
                        currentCount += 1;
                        await dbUserUpdate(userId, { block_count: currentCount }, env);
                        
                        const blockNotification = `⚠️ 您的消息触发了屏蔽关键词过滤器 (${currentCount}/${blockThreshold}次)，此消息已被丢弃，不会转发给对方。`;
                        
                        if (currentCount >= blockThreshold) {
                            await dbUserUpdate(userId, { is_blocked: true }, env);
                            const autoBlockMessage = `❌ 您已多次触发屏蔽关键词，根据设置，您已被自动屏蔽。机器人将不再接收您的任何消息。`;
                            
                            await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: chatId, text: blockNotification });
                            await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: chatId, text: autoBlockMessage });
                            return;
                        }
                        
                        await telegramApi(env.BOT_TOKEN, "sendMessage", {
                            chat_id: chatId,
                            text: blockNotification,
                        });

                        return; 
                    }
                } catch(e) {}
            }
        }

        // --- [转发内容过滤检查] ---
        const filters = {
            media: (await getConfig('enable_image_forwarding', env, 'true')).toLowerCase() === 'true',
            link: (await getConfig('enable_link_forwarding', env, 'true')).toLowerCase() === 'true',
            text: (await getConfig('enable_text_forwarding', env, 'true')).toLowerCase() === 'true',
            channel_forward: (await getConfig('enable_channel_forwarding', env, 'true')).toLowerCase() === 'true', 
            any_forward: (await getConfig('enable_forward_forwarding', env, 'true')).toLowerCase() === 'true', 
            audio_voice: (await getConfig('enable_audio_forwarding', env, 'true')).toLowerCase() === 'true', 
            sticker_gif: (await getConfig('enable_sticker_forwarding', env, 'true')).toLowerCase() === 'true', 
        };

        let isForwardable = true;
        let filterReason = '';

        const hasLinks = (msg) => {
            const entities = msg.entities || msg.caption_entities || [];
            return entities.some(entity => entity.type === 'url' || entity.type === 'text_link');
        };

        if (message.forward_from || message.forward_from_chat) {
             if (!filters.any_forward) {
                isForwardable = false;
                filterReason = '转发消息 (来自用户/群组/频道)';
            } 
            else if (message.forward_from_chat && message.forward_from_chat.type === 'channel' && !filters.channel_forward) {
                isForwardable = false;
                filterReason = '频道转发消息';
            }
        } 
        else if (message.audio || message.voice) {
            if (!filters.audio_voice) {
                isForwardable = false;
                filterReason = '音频或语音消息';
            }
        }
        else if (message.sticker || message.animation) {
             if (!filters.sticker_gif) {
                isForwardable = false;
                filterReason = '贴纸或GIF';
            }
        }
        else if (message.photo || message.video || message.document) {
            if (!filters.media) {
                isForwardable = false;
                filterReason = '媒体内容（图片/视频/文件）';
            }
        } 
        
        if (isForwardable && hasLinks(message)) {
            if (!filters.link) {
                isForwardable = false;
                filterReason = filterReason ? `${filterReason} (并包含链接)` : '包含链接的内容';
            }
        }

        const isTextWithNoMedia = message.text && 
                           !message.photo && !message.video && !message.document && 
                           !message.sticker && !message.audio && !message.voice && 
                           !message.forward_from_chat && !message.forward_from && !message.animation; 
        
        if (isForwardable && isTextWithNoMedia) {
            if (!filters.text) {
                isForwardable = false;
                filterReason = '纯文本内容';
            }
        }

        if (!isForwardable) {
            const filterNotification = `此消息已被过滤：${filterReason}。根据设置，此类内容不会转发给对方。`;
            await telegramApi(env.BOT_TOKEN, "sendMessage", {
                chat_id: chatId,
                text: filterNotification,
            });
            return; 
        }
        
        const autoResponseRules = await getAutoReplyRules(env); 
        if (autoResponseRules.length > 0 && text) { 
            for (const rule of autoResponseRules) {
                try {
                    const regex = new RegExp(rule.keywords, 'gi'); 
                    if (regex.test(text)) {
                        const autoReplyPrefix = "此消息为自动回复\n\n";
                        await telegramApi(env.BOT_TOKEN, "sendMessage", {
                            chat_id: chatId,
                            text: autoReplyPrefix + rule.response,
                        });
                        return; 
                    }
                } catch(e) {}
            }
        }
        
        await handleRelayToTopic(message, user, env); 
        
    } else {
        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: chatId,
            text: "您的状态异常，请使用 /start 命令重试。",
        });
    }
}

/**
 * [修改] L1 验证流程入口 (使用 web_app 按钮)
 */
async function handleStart(chatId, env) {
    const user = await dbUserGetOrCreate(chatId, env);
    
    if (!env.WORKER_URL || !env.TURNSTILE_SITE_KEY) {
        await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: chatId, text: "⚠️ 机器人配置错误：Cloudflare Turnstile 未正确配置。请联系管理员。" });
        console.error("handleStart: Missing WORKER_URL or TURNSTILE_SITE_KEY");
        return;
    }

    switch (user.user_state) {
        case 'new':
        case 'pending_turnstile':
            const workerUrl = env.WORKER_URL.replace(/\/$/, ''); 
            const verificationUrl = `${workerUrl}/verify?user_id=${chatId}`;
            const welcomeMessage = await getConfig('welcome_msg', env, "欢迎！在使用之前，请先完成人机验证。");

            const text = welcomeMessage + "\n\n请点击下方按钮，开始人机验证。";
            // [修改] 将按钮类型改为 web_app
            const keyboard = { 
                inline_keyboard: [[
                    { 
                        text: "🛡️ 点击开始人机验证", 
                        web_app: { url: verificationUrl } 
                    }
                ]] 
            };

            await telegramApi(env.BOT_TOKEN, "sendMessage", {
                chat_id: chatId,
                text: text,
                reply_markup: keyboard
            });
            
            if (user.user_state === 'new') {
                 await dbUserUpdate(chatId, { user_state: "pending_turnstile" }, env);
            }
            break;
        
        case 'pending_verification':
            const defaultVerifQ = "问题：1+1=?\n\n提示：\n1. 正确答案不是“2”。\n2. 答案在机器人简介内，请看简介的答案进行回答。";
            const verificationQuestion = await getConfig('verif_q', env, defaultVerifQ);
            
            await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: chatId, text: "您已通过第一道验证。请回答以下第二道问题：\n\n" + verificationQuestion });
            break;

        case 'verified':
            await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: chatId, text: "您已通过验证，可以正常发送消息。" });
            break;
    }
}

async function handleVerification(chatId, answer, env) {
    const expectedAnswer = await getConfig('verif_a', env, "3"); 

    if (answer.trim() === expectedAnswer.trim()) {
        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: chatId,
            text: "✅ L2 验证通过！您现在可以发送消息了。\n\n**注意：您的第一条消息必须是纯文本内容。**",
            parse_mode: "Markdown",
        });
        await dbUserUpdate(chatId, { user_state: "verified" }, env);
    } else {
        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: chatId,
            text: "❌ L2 验证失败！\n请查看机器人简介查找答案，然后重新回答。",
        });
    }
}

// --- 管理员配置主菜单逻辑 (使用 D1) ---

async function handleAdminConfigStart(chatId, env) {
    const isPrimary = isPrimaryAdmin(chatId, env);
    if (!isPrimary) {
        await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: chatId, text: "您是授权协管员，已绕过验证。此菜单仅供主管理员使用。", });
        return;
    }
    
    const menuText = `
⚙️ <b>机器人主配置菜单</b>

请选择要管理的配置类别：
    `.trim();

    const menuKeyboard = {
        inline_keyboard: [
            [{ text: "📝 基础配置 (验证问答)", callback_data: "config:menu:base" }],
            [{ text: "🤖 自动回复管理", callback_data: "config:menu:autoreply" }],
            [{ text: "🚫 关键词屏蔽管理", callback_data: "config:menu:keyword" }],
            [{ text: "🛠 过滤与系统功能", callback_data: "config:menu:filter" }],
            [{ text: "🧑‍💻 协管员授权设置", callback_data: "config:menu:authorized" }], 
            [{ text: "💾 备份群组设置", callback_data: "config:menu:backup" }], 
            [{ text: "🔄 刷新主菜单", callback_data: "config:menu" }],
        ]
    };

    await dbAdminStateDelete(chatId, env);

    if (env.last_config_message_id) {
        await telegramApi(env.BOT_TOKEN, "editMessageText", {
            chat_id: chatId,
            message_id: env.last_config_message_id,
            text: menuText,
            parse_mode: "HTML",
            reply_markup: menuKeyboard,
        }).catch(e => {});
        return;
    }


    await telegramApi(env.BOT_TOKEN, "sendMessage", {
        chat_id: chatId,
        text: menuText,
        parse_mode: "HTML",
        reply_markup: menuKeyboard,
    });
}

async function handleAdminBaseConfigMenu(chatId, messageId, env) {
    const welcomeMsg = await getConfig('welcome_msg', env, "欢迎！...");
    const verifQ = await getConfig('verif_q', env, "问题：1+1=?...");
    const verifA = await getConfig('verif_a', env, "3");

    const menuText = `
⚙️ <b>基础配置 (人机验证)</b>

<b>当前设置:</b>
• 欢迎消息: ${escapeHtml(welcomeMsg).substring(0, 30)}...
• 验证问题: ${escapeHtml(verifQ).substring(0, 30)}...
• 验证答案: <code>${escapeHtml(verifA)}</code>

请选择要修改的配置项:
    `.trim();

    const menuKeyboard = {
        inline_keyboard: [
            [{ text: "📝 编辑欢迎消息", callback_data: "config:edit:welcome_msg" }],
            [{ text: "❓ 编辑验证问题", callback_data: "config:edit:verif_q" }],
            [{ text: "🔑 编辑验证答案", callback_data: "config:edit:verif_a" }],
            [{ text: "⬅️ 返回主菜单", callback_data: "config:menu" }],
        ]
    };

    const apiMethod = (messageId && messageId !== 0) ? "editMessageText" : "sendMessage";
    const params = {
        chat_id: chatId,
        text: menuText,
        parse_mode: "HTML",
        reply_markup: menuKeyboard,
    };
    if (apiMethod === "editMessageText") {
        params.message_id = messageId;
    }
    await telegramApi(env.BOT_TOKEN, apiMethod, params);
}

async function handleAdminAuthorizedConfigMenu(chatId, messageId, env) {
    const primaryAdmins = env.ADMIN_IDS ? env.ADMIN_IDS.split(',').map(id => id.trim()).filter(id => id !== "") : [];
    const authorizedAdmins = await getAuthorizedAdmins(env);
    
    const allAdmins = [...new Set([...primaryAdmins, ...authorizedAdmins])]; 
    const authorizedCount = authorizedAdmins.length;

    const menuText = `
🧑‍💻 <b>协管员授权设置</b>

<b>主管理员 (来自 ENV):</b> <code>${primaryAdmins.join(', ')}</code>
<b>已授权协管员 (来自 D1):</b> <code>${authorizedAdmins.join(', ') || '无'}</code>
<b>总管理员/协管员数量:</b> ${allAdmins.length} 人

<b>注意：</b>
1. 协管员 ID 或用户名必须与群组话题中的回复者一致。
2. 协管员的私聊会自动绕过验证。
3. 输入格式：ID 或用户名，多个用逗号分隔。

请选择要修改的配置项:
    `.trim();

    const menuKeyboard = {
        inline_keyboard: [
            [{ text: "✏️ 设置/修改协管员列表", callback_data: "config:edit:authorized_admins" }],
            [{ text: `🗑️ 清空协管员列表 (${authorizedCount}人)`, callback_data: "config:edit:authorized_admins_clear" }],
            [{ text: "⬅️ 返回主菜单", callback_data: "config:menu" }],
        ]
    };

    const apiMethod = (messageId && messageId !== 0) ? "editMessageText" : "sendMessage";
    const params = {
        chat_id: chatId,
        text: menuText,
        parse_mode: "HTML",
        reply_markup: menuKeyboard,
    };
    if (apiMethod === "editMessageText") {
        params.message_id = messageId;
    }
    await telegramApi(env.BOT_TOKEN, apiMethod, params);
}

async function handleAdminAutoReplyMenu(chatId, messageId, env) {
    const rules = await getAutoReplyRules(env);
    const ruleCount = rules.length;
    
    const menuText = `
🤖 <b>自动回复管理</b>

当前规则总数：<b>${ruleCount}</b> 条。

请选择操作：
    `.trim();

    const menuKeyboard = {
        inline_keyboard: [
            [{ text: "➕ 新增自动回复规则", callback_data: "config:add:keyword_responses" }],
            [{ text: `🗑️ 管理/删除现有规则 (${ruleCount}条)`, callback_data: "config:list:keyword_responses" }],
            [{ text: "⬅️ 返回主菜单", callback_data: "config:menu" }],
        ]
    };

    const apiMethod = (messageId && messageId !== 0) ? "editMessageText" : "sendMessage";
    const params = {
        chat_id: chatId,
        text: menuText,
        parse_mode: "HTML",
        reply_markup: menuKeyboard,
    };
    if (apiMethod === "editMessageText") {
        params.message_id = messageId;
    }
    await telegramApi(env.BOT_TOKEN, apiMethod, params);
}

async function handleAdminKeywordBlockMenu(chatId, messageId, env) {
    const blockKeywords = await getBlockKeywords(env);
    const keywordCount = blockKeywords.length;
    const blockThreshold = await getConfig('block_threshold', env, "5");

    const menuText = `
🚫 <b>关键词屏蔽管理</b>

当前屏蔽关键词总数：<b>${keywordCount}</b> 个。
屏蔽次数阈值：<code>${escapeHtml(blockThreshold)}</code> 次。

请选择操作：
    `.trim();

    const menuKeyboard = {
        inline_keyboard: [
            [{ text: "➕ 新增屏蔽关键词", callback_data: "config:add:block_keywords" }],
            [{ text: `🗑️ 管理/删除现有关键词 (${keywordCount}个)`, callback_data: "config:list:block_keywords" }],
            [{ text: "✏️ 修改屏蔽次数阈值", callback_data: "config:edit:block_threshold" }],
            [{ text: "⬅️ 返回主菜单", callback_data: "config:menu" }],
        ]
    };

    const apiMethod = (messageId && messageId !== 0) ? "editMessageText" : "sendMessage";
    const params = {
        chat_id: chatId,
        text: menuText,
        parse_mode: "HTML",
        reply_markup: menuKeyboard,
    };
    if (apiMethod === "editMessageText") {
        params.message_id = messageId;
    }
    await telegramApi(env.BOT_TOKEN, apiMethod, params);
}

async function handleAdminBackupConfigMenu(chatId, messageId, env) {
    const backupGroupId = await getConfig('backup_group_id', env, "未设置"); 
    const backupStatus = backupGroupId !== "未设置" && backupGroupId !== "" ? "✅ 已启用" : "❌ 未启用";

    const menuText = `
💾 <b>备份群组设置</b>

<b>当前设置:</b>
• 状态: ${backupStatus}
• 备份群组 ID: <code>${escapeHtml(backupGroupId)}</code>

<b>注意：</b>此群组仅用于备份消息，不参与管理员回复中继等互动功能。
群组 ID 可以是数字 ID 或 \`@group_username\`。如果设置为空，则禁用备份。

请选择要修改的配置项:
    `.trim();

    const menuKeyboard = {
        inline_keyboard: [
            [{ text: "✏️ 设置/修改备份群组 ID", callback_data: "config:edit:backup_group_id" }],
            [{ text: "❌ 清除备份群组 ID (禁用备份)", callback_data: "config:edit:backup_group_id_clear" }],
            [{ text: "⬅️ 返回主菜单", callback_data: "config:menu" }],
        ]
    };

    const apiMethod = (messageId && messageId !== 0) ? "editMessageText" : "sendMessage";
    const params = {
        chat_id: chatId,
        text: menuText,
        parse_mode: "HTML",
        reply_markup: menuKeyboard,
    };
    if (apiMethod === "editMessageText") {
        params.message_id = messageId;
    }
    await telegramApi(env.BOT_TOKEN, apiMethod, params);
}


async function handleAdminRuleList(chatId, messageId, env, key) {
    let rules = [];
    let menuText = "";
    let backCallback = "";

    if (key === 'keyword_responses') {
        rules = await getAutoReplyRules(env);
        menuText = `
🤖 <b>自动回复规则列表 (${rules.length}条)</b>

请点击右侧按钮删除对应规则。
规则格式：<code>关键词表达式</code> ➡️ <code>回复内容</code>
---
        `.trim();
        backCallback = "config:menu:autoreply";
    } else if (key === 'block_keywords') {
        rules = await getBlockKeywords(env);
        menuText = `
🚫 <b>屏蔽关键词列表 (${rules.length}个)</b>

请点击右侧按钮删除对应关键词。
关键词格式：<code>关键词表达式</code>
---
        `.trim();
        backCallback = "config:menu:keyword";
    } else {
        return;
    }

    const ruleButtons = [];
    if (rules.length === 0) {
        menuText += "\n\n<i>（列表为空）</i>";
    } else {
        rules.forEach((rule, index) => {
            let label = "";
            let deleteId = "";
            
            if (key === 'keyword_responses') {
                const keywordsSnippet = rule.keywords.substring(0, 15);
                const responseSnippet = rule.response.substring(0, 20);
                label = `${index + 1}. <code>${escapeHtml(keywordsSnippet)}...</code> ➡️ ${escapeHtml(responseSnippet)}...`;
                deleteId = rule.id;
            } else if (key === 'block_keywords') {
                const keywordSnippet = rule.substring(0, 25);
                label = `${index + 1}. <code>${escapeHtml(keywordSnippet)}...</code>`;
                deleteId = rule; 
            }
            
            menuText += `\n${label}`;

            ruleButtons.push([
                { 
                    text: `🗑️ 删除 ${index + 1}`, 
                    callback_data: `config:delete:${key}:${deleteId}` 
                }
            ]);
        });
    }

    ruleButtons.push([{ text: "⬅️ 返回管理菜单", callback_data: backCallback }]);

    const apiMethod = (messageId && messageId !== 0) ? "editMessageText" : "sendMessage";
    const params = {
        chat_id: chatId,
        text: menuText,
        parse_mode: "HTML",
        reply_markup: { inline_keyboard: ruleButtons },
    };
    if (apiMethod === "editMessageText") {
        params.message_id = messageId;
    }
    await telegramApi(env.BOT_TOKEN, apiMethod, params);
}

async function handleAdminRuleDelete(chatId, messageId, env, key, id) {
    let rules = [];
    let typeName = "";
    let backCallback = "";

    if (key === 'keyword_responses') {
        rules = await getAutoReplyRules(env);
        typeName = "自动回复规则";
        backCallback = "config:menu:autoreply";
        rules = rules.filter(rule => rule.id.toString() !== id.toString());
    } else if (key === 'block_keywords') {
        rules = await getBlockKeywords(env);
        typeName = "屏蔽关键词";
        backCallback = "config:menu:keyword";
        rules = rules.filter(keyword => keyword !== id);
    } else {
        return;
    }

    await dbConfigPut(key, JSON.stringify(rules), env);

    await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: chatId, text: `✅ ${typeName}已删除并更新。`, show_alert: false });
    await handleAdminRuleList(chatId, messageId, env, key);
}


async function handleAdminTypeBlockMenu(chatId, messageId, env) {
    const mediaStatus = (await getConfig('enable_image_forwarding', env, 'true')).toLowerCase() === 'true'; 
    const linkStatus = (await getConfig('enable_link_forwarding', env, 'true')).toLowerCase() === 'true';
    const textStatus = (await getConfig('enable_text_forwarding', env, 'true')).toLowerCase() === 'true';
    
    const channelForwardStatus = (await getConfig('enable_channel_forwarding', env, 'true')).toLowerCase() === 'true'; 
    const anyForwardStatus = (await getConfig('enable_forward_forwarding', env, 'true')).toLowerCase() === 'true'; 
    const audioVoiceStatus = (await getConfig('enable_audio_forwarding', env, 'true')).toLowerCase() === 'true'; 
    const stickerGifStatus = (await getConfig('enable_sticker_forwarding', env, 'true')).toLowerCase() === 'true'; 

    const adminReceiptStatus = (await getConfig('enable_admin_receipt', env, 'true')).toLowerCase() === 'true';

    const statusToText = (status) => status ? "✅ 允许/开启" : "❌ 屏蔽/关闭";
    const statusToCallback = (key, status) => `config:toggle:${key}:${status ? 'false' : 'true'}`;

    const menuText = `
🛠 <b>过滤与系统功能设置</b>

点击按钮切换状态 (切换后立即生效)。

<b>系统功能:</b>
| 功能 | 状态 |
| :--- | :--- |
| 管理员回复回执 | ${statusToText(adminReceiptStatus)} |

<b>消息转发过滤:</b>
| 类型 | 状态 |
| :--- | :--- |
| <b>转发消息（用户/群组/频道）</b>| ${statusToText(anyForwardStatus)} |
| 频道转发消息 (细分) | ${statusToText(channelForwardStatus)} |
| <b>音频/语音消息</b> | ${statusToText(audioVoiceStatus)} |
| <b>贴纸/GIF (动画)</b> | ${statusToText(stickerGifStatus)} |
| 图片/视频/文件 | ${statusToText(mediaStatus)} |
| 链接消息 | ${statusToText(linkStatus)} |
| 纯文本消息 | ${statusToText(textStatus)} |
    `.trim();

    const menuKeyboard = {
        inline_keyboard: [
            [{ text: `管理员回复回执: ${statusToText(adminReceiptStatus)}`, callback_data: statusToCallback('enable_admin_receipt', adminReceiptStatus) }],
            
            [{ text: `转发消息 (用户/群组/频道): ${statusToText(anyForwardStatus)}`, callback_data: statusToCallback('enable_forward_forwarding', anyForwardStatus) }],
            [{ text: `音频/语音消息 (Audio/Voice): ${statusToText(audioVoiceStatus)}`, callback_data: statusToCallback('enable_audio_forwarding', audioVoiceStatus) }],
            [{ text: `贴纸/GIF (Sticker/Animation): ${statusToText(stickerGifStatus)}`, callback_data: statusToCallback('enable_sticker_forwarding', stickerGifStatus) }],
            
            [{ text: `图片/视频/文件 (Photo/Video/Doc): ${statusToText(mediaStatus)}`, callback_data: statusToCallback('enable_image_forwarding', mediaStatus) }],
            [{ text: `频道转发消息 (Channel Forward): ${statusToCallback('enable_channel_forwarding', channelForwardStatus)}`, callback_data: statusToCallback('enable_channel_forwarding', channelForwardStatus) }],
            [{ text: `链接消息 (URL/TextLink): ${statusToText(linkStatus)}`, callback_data: statusToCallback('enable_link_forwarding', linkStatus) }],
            [{ text: `纯文本消息 (Pure Text): ${statusToText(textStatus)}`, callback_data: statusToCallback('enable_text_forwarding', textStatus) }],

            [{ text: "⬅️ 返回主菜单", callback_data: "config:menu" }],
        ]
    };


    const apiMethod = (messageId && messageId !== 0) ? "editMessageText" : "sendMessage";
    const params = {
        chat_id: chatId,
        text: menuText,
        parse_mode: "HTML",
        reply_markup: menuKeyboard,
    };
    if (apiMethod === "editMessageText") {
        params.message_id = messageId;
    }
    await telegramApi(env.BOT_TOKEN, apiMethod, params);
}


async function handleAdminConfigInput(userId, text, adminStateJson, env) {
    const adminState = JSON.parse(adminStateJson);

    if (text.toLowerCase() === "/cancel") {
        await dbAdminStateDelete(userId, env);
        await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: userId, text: "✅ 编辑已取消。", });
        await handleAdminConfigStart(userId, env); 
        return;
    }
    
    if (adminState.action === 'awaiting_input' && adminState.key) {
        
        let successMsg = "";
        let finalValue = text;

        if (adminState.key === 'verif_a' || adminState.key === 'block_threshold') {
            finalValue = text.trim(); 
        } else if (adminState.key === 'backup_group_id') {
            finalValue = text.trim();
        } else if (adminState.key === 'authorized_admins') {
            const adminList = text.split(',').map(id => id.trim()).filter(id => id !== "");
            finalValue = JSON.stringify(adminList); 
        }

        if (adminState.key === 'block_keywords_add') {
            const blockKeywords = await getBlockKeywords(env);
            const newKeyword = finalValue.trim();
            if (newKeyword && !blockKeywords.includes(newKeyword)) {
                blockKeywords.push(newKeyword);
                await dbConfigPut('block_keywords', JSON.stringify(blockKeywords), env);
                successMsg = `✅ 屏蔽关键词 <code>${escapeHtml(newKeyword)}</code> 已添加。`;
            } else {
                 successMsg = `⚠️ 屏蔽关键词未添加，内容为空或已存在。`;
            }
            await dbAdminStateDelete(userId, env);
            await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: userId, text: successMsg, parse_mode: "HTML" });
            await handleAdminKeywordBlockMenu(userId, 0, env); 
            return;
        } else if (adminState.key === 'keyword_responses_add') {
            const rules = await getAutoReplyRules(env);
            
            const parts = finalValue.split('===');
            if (parts.length === 2 && parts[0].trim() && parts[1].trim()) {
                const newRule = {
                    keywords: parts[0].trim(),
                    response: parts[1].trim(),
                    id: Date.now(), 
                };
                rules.push(newRule);
                await dbConfigPut('keyword_responses', JSON.stringify(rules), env);
                successMsg = `✅ 自动回复规则已添加。关键词: <code>${escapeHtml(newRule.keywords)}</code>`;
            } else {
                 successMsg = `⚠️ 自动回复规则未添加。请确保格式正确：<code>关键词表达式===回复内容</code>`;
            }
            await dbAdminStateDelete(userId, env);
            await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: userId, text: successMsg, parse_mode: "HTML" });
            await handleAdminAutoReplyMenu(userId, 0, env); 
            return;
        }

        await dbConfigPut(adminState.key, finalValue, env);
        
        await dbAdminStateDelete(userId, env);
        
        switch (adminState.key) {
            case 'welcome_msg': successMsg = `✅ <b>欢迎消息</b>已更新。`; break;
            case 'verif_q': successMsg = `✅ <b>验证问题</b>已更新。`; break;
            case 'verif_a': successMsg = `✅ <b>验证答案</b>已更新为：<code>${escapeHtml(finalValue)}</code>`; break;
            case 'block_threshold': successMsg = `✅ <b>屏蔽次数阈值</b>已更新为：<code>${escapeHtml(finalValue)}</code>`; break;
            case 'backup_group_id': 
                if (finalValue === '') {
                    successMsg = `✅ <b>备份群组 ID</b>已清除，备份功能已禁用。`;
                } else {
                    successMsg = `✅ <b>备份群组 ID</b>已更新为：<code>${escapeHtml(finalValue)}</code>`; 
                }
                break;
            case 'authorized_admins': {
                const authorizedAdmins = JSON.parse(finalValue);
                if (authorizedAdmins.length === 0) {
                     successMsg = `✅ <b>协管员授权列表</b>已清空。`;
                } else {
                     successMsg = `✅ <b>协管员授权列表</b>已更新，共授权 ${authorizedAdmins.length} 人。`;
                }
                break;
            }
            default: successMsg = "✅ 配置已更新。"; break;
        }

        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: userId,
            text: successMsg,
            parse_mode: "HTML",
        });

        let nextMenuAction = 'config:menu';
        if (['welcome_msg', 'verif_q', 'verif_a'].includes(adminState.key)) {
            nextMenuAction = 'config:menu:base';
        } else if (adminState.key === 'block_threshold') {
            nextMenuAction = 'config:menu:keyword';
        } else if (adminState.key === 'backup_group_id') {
            nextMenuAction = 'config:menu:backup';
        } else if (adminState.key === 'authorized_admins') {
            nextMenuAction = 'config:menu:authorized';
        }
        
        if (nextMenuAction === 'config:menu:base') {
            await handleAdminBaseConfigMenu(userId, 0, env); 
        } else if (nextMenuAction === 'config:menu:autoreply') {
             await handleAdminAutoReplyMenu(userId, 0, env); 
        } else if (nextMenuAction === 'config:menu:keyword') {
             await handleAdminKeywordBlockMenu(userId, 0, env); 
        } else if (nextMenuAction === 'config:menu:backup') {
             await handleAdminBackupConfigMenu(userId, 0, env); 
        } else if (nextMenuAction === 'config:menu:authorized') {
             await handleAdminAuthorizedConfigMenu(userId, 0, env); 
        } else {
             await handleAdminConfigStart(userId, env); 
        }


    } else {
        await dbAdminStateDelete(userId, env);
        await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: userId, text: "⚠️ 状态错误，已重置。请重新使用 /start 访问菜单。", });
    }
}


async function handleRelayToTopic(message, user, env) { 
    const { from: userDetails, date } = message;
    const { userId, topicName, infoCard } = getUserInfo(userDetails, date);
    let topicId = user.topic_id;
    const isBlocked = user.is_blocked;

    const createTopicForUser = async () => {
        try {
            const newTopic = await telegramApi(env.BOT_TOKEN, "createForumTopic", {
                chat_id: env.ADMIN_GROUP_ID,
                name: topicName,
            });
            const newTopicId = newTopic.message_thread_id.toString();
            const { name, username } = getUserInfo(userDetails, date);
            const newInfo = { name, username, first_message_timestamp: date };

            await dbUserUpdate(userId, { 
                topic_id: newTopicId,
                user_info: newInfo 
            }, env);

            await telegramApi(env.BOT_TOKEN, "sendMessage", {
                chat_id: env.ADMIN_GROUP_ID,
                text: infoCard,
                message_thread_id: newTopicId,
                parse_mode: "HTML",
                reply_markup: getInfoCardButtons(userId, isBlocked), 
            });

            return newTopicId;
        } catch (e) {
            console.error("createTopicForUser 创建话题失败:", e?.message || e);
            throw e;
        }
    };

    if (!topicId) {
        try {
            topicId = await createTopicForUser();
        } catch (e) {
            await telegramApi(env.BOT_TOKEN, "sendMessage", {
                chat_id: userId,
                text: "抱歉，无法连接客服（创建话题失败）。请稍后再试。",
            });
            return;
        }
    }

    const tryCopyToTopic = async (targetTopicId) => {
        try {
            const result = await telegramApi(env.BOT_TOKEN, "copyMessage", {
                chat_id: env.ADMIN_GROUP_ID,
                from_chat_id: userId,
                message_id: message.message_id,
                message_thread_id: targetTopicId,
            });
            return result;
        } catch (e) {
            if (e.message.includes("message thread not found") || e.message.includes("chat not found")) {
                 console.warn(`话题 ${targetTopicId} 不存在/无效。`);
            } else {
                 console.error(`tryCopyToTopic 到话题 ${targetTopicId} 失败:`, e?.message || e);
            }
            throw e;
        }
    };

    try {
        await tryCopyToTopic(topicId);
    } catch (e) {
        try {
            await dbUserUpdate(userId, { topic_id: null }, env);
            
            const newTopicId = await createTopicForUser();
            try {
                await tryCopyToTopic(newTopicId);
            } catch (e2) {
                console.error("尝试将消息复制到新话题也失败:", e2?.message || e2);
                await telegramApi(env.BOT_TOKEN, "sendMessage", {
                    chat_id: userId,
                    text: "抱歉，消息转发失败（请稍后再试或联系管理员）。",
                });
                return;
            }
        } catch (createErr) {
            console.error("在处理话题失效时，创建新话题失败:", createErr?.message || createErr);
            await telegramApi(env.BOT_TOKEN, "sendMessage", {
                chat_id: userId,
                text: "抱歉，无法创建新的客服话题（请稍后再试）。",
            });
            return;
        }
    }

    await telegramApi(env.BOT_TOKEN, "sendMessage", {
        chat_id: userId,
        text: "✅ 你的消息已发送给管理员，请耐心等待回复。",
        reply_to_message_id: message.message_id,
        disable_notification: true,
    }).catch(e => {}); 

    if (!user.first_message_sent) {
        await dbUserUpdate(userId, { first_message_sent: true }, env);
    }

    if (message.text) {
        const messageData = {
            text: message.text,
            date: message.date
        };
        await dbMessageDataPut(userId, message.message_id.toString(), messageData, env);
    }
    
    const backupGroupId = await getConfig('backup_group_id', env, "");
    if (backupGroupId) {
        const userInfo = getUserInfo(message.from, user.date); 

        const fromUserHeader = `
<b>--- 备份消息 ---</b>
👤 <b>来自用户:</b> <a href="tg://user?id=${userInfo.userId}">${userInfo.name || '无昵称'}</a>
• ID: <code>${userInfo.userId}</code>
• 用户名: ${userInfo.username}
------------------
`.trim() + '\n\n'; 
        
        const backupParams = {
            chat_id: backupGroupId,
            disable_notification: true, 
            parse_mode: "HTML",
        };

        try {
            if (message.text) {
                const combinedText = fromUserHeader + message.text;
                await telegramApi(env.BOT_TOKEN, "sendMessage", {
                    ...backupParams,
                    text: combinedText,
                });
                return; 
            }

            let apiMethod = null; 
            let payload = { ...backupParams };
            let fileId = null;
            let originalCaption = message.caption || "";
            let newCaption = fromUserHeader + originalCaption;

            if (message.photo && message.photo.length) {
                apiMethod = "sendPhoto";
                fileId = message.photo[message.photo.length - 1].file_id;
                payload.photo = fileId;
                payload.caption = newCaption;
            } else if (message.video) {
                apiMethod = "sendVideo";
                fileId = message.video.file_id;
                payload.video = fileId;
                payload.caption = newCaption;
            } else if (message.document) {
                apiMethod = "sendDocument";
                fileId = message.document.file_id;
                payload.document = fileId;
                payload.caption = newCaption;
            } else if (message.audio) {
                apiMethod = "sendAudio";
                fileId = message.audio.file_id;
                payload.audio = fileId;
                payload.caption = newCaption;
            } else if (message.voice) {
                apiMethod = "sendVoice";
                fileId = message.voice.file_id;
                payload.voice = fileId;
                payload.caption = newCaption;
            } else if (message.animation) {
                apiMethod = "sendAnimation";
                fileId = message.animation.file_id;
                payload.animation = fileId;
                payload.caption = newCaption;
            } 
            
            if (apiMethod && fileId) {
                await telegramApi(env.BOT_TOKEN, apiMethod, payload);
                return; 
            }

            if (message.sticker || message.poll || message.game || message.forward_from_chat || message.forward_from || message.contact || message.location || message.venue || message.invoice) {
                
                await telegramApi(env.BOT_TOKEN, "sendMessage", {
                    ...backupParams,
                    text: fromUserHeader.trim(), 
                    parse_mode: "HTML",
                });

                await telegramApi(env.BOT_TOKEN, "copyMessage", {
                    chat_id: backupGroupId,
                    from_chat_id: userId,
                    message_id: message.message_id,
                });
                return; 
            }

        } catch (e) {
            console.error("消息备份转发失败:", e?.message || e);
        }
    }
}

async function handleRelayEditedMessage(editedMessage, env) {
    const { from: user } = editedMessage;
    const userId = user.id.toString();
    
    const userData = await dbUserGetOrCreate(userId, env);
    const topicId = userData.topic_id;

    if (!topicId) {
        return; 
    }

    const storedData = await dbMessageDataGet(userId, editedMessage.message_id.toString(), env);
    let originalText = "[原始内容无法获取/非文本内容]";
    let originalDate = "[发送时间无法获取]";
    
    if (storedData) {
        originalText = storedData.text || originalText;
        originalDate = new Date(storedData.date * 1000).toLocaleString('zh-CN');

        const updatedData = { 
            text: editedMessage.text || editedMessage.caption || '',
            date: storedData.date 
        };
        await dbMessageDataPut(userId, editedMessage.message_id.toString(), updatedData, env);
    }

    const newContent = editedMessage.text || editedMessage.caption || "[非文本/媒体说明内容]";
    
    const notificationText = `
⚠️ <b>用户消息已修改</b>
---
<b>原始信息:</b> 
<code>${escapeHtml(originalText)}</code>

<b>原消息发送时间:</b> 
<code>${originalDate}</code>

<b>修改后的新内容:</b>
${escapeHtml(newContent)}
    `.trim();
    
    try {
        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: env.ADMIN_GROUP_ID,
            text: notificationText,
            message_thread_id: topicId,
            parse_mode: "HTML", 
        });
        
    } catch (e) {
        console.error("处理已编辑消息失败:", e.message);
    }
}

async function handlePinCard(callbackQuery, message, env) {
    const topicId = message.message_thread_id; 
    const adminGroupId = message.chat.id;
    const messageIdToPin = message.message_id; 

    try {
        await telegramApi(env.BOT_TOKEN, "pinChatMessage", {
            chat_id: adminGroupId,
            message_id: messageIdToPin,
            message_thread_id: topicId, 
            disable_notification: true, 
        });

        await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", {
            callback_query_id: callbackQuery.id,
            text: `📌 资料卡已在话题中置顶。`,
            show_alert: false 
        });

    } catch (e) {
         console.error("处理置顶操作失败:", e.message);
         await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", {
            callback_query_id: callbackQuery.id,
            text: `❌ 置顶失败。请确保机器人或有群组的置顶权限。错误信息: ${e.message}`,
            show_alert: true
        });
    }
}


async function handleCallbackQuery(callbackQuery, env) {
    const { data, message, from: user } = callbackQuery;
    const chatId = message.chat.id.toString();
    const isPrimary = isPrimaryAdmin(user.id, env); 

    if (data.startsWith('config:')) {
        if (!isPrimary) {
            await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: callbackQuery.id, text: "您不是主管理员，没有权限执行此操作。", show_alert: true });
            return;
        }
        
        const parts = data.split(':'); 
        const actionType = parts[1]; 
        const keyOrAction = parts[2]; 
        const value = parts[3]; 

        await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: callbackQuery.id, text: "处理中...", show_alert: false });

        if (actionType === 'menu') {
            if (keyOrAction === 'base') {
                await handleAdminBaseConfigMenu(chatId, message.message_id, env);
            } else if (keyOrAction === 'autoreply') {
                await handleAdminAutoReplyMenu(chatId, message.message_id, env);
            } else if (keyOrAction === 'keyword') {
                await handleAdminKeywordBlockMenu(chatId, message.message_id, env);
            } else if (keyOrAction === 'filter') {
                await handleAdminTypeBlockMenu(chatId, message.message_id, env);
            } else if (keyOrAction === 'backup') {
                await handleAdminBackupConfigMenu(chatId, message.message_id, env);
            } else if (keyOrAction === 'authorized') {
                await handleAdminAuthorizedConfigMenu(chatId, message.message_id, env);
            } else { 
                await handleAdminConfigStart(chatId, env);
            }
        } else if (actionType === 'toggle' && keyOrAction && value) {
            await dbConfigPut(keyOrAction, value, env);
            await handleAdminTypeBlockMenu(chatId, message.message_id, env); 
        } else if (actionType === 'edit' && keyOrAction) {
            
            if (keyOrAction === 'backup_group_id_clear') {
                await dbConfigPut('backup_group_id', '', env); 
                await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: callbackQuery.id, text: `✅ 备份群组 ID 已清除，备份功能已禁用。`, show_alert: false });
                await handleAdminBackupConfigMenu(chatId, message.message_id, env); 
                return;
            }
            
             if (keyOrAction === 'authorized_admins_clear') {
                await dbConfigPut('authorized_admins', '[]', env); 
                await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: callbackQuery.id, text: `✅ 协管员授权列表已清空。`, show_alert: false });
                await handleAdminAuthorizedConfigMenu(chatId, message.message_id, env); 
                return;
            }
            
            await dbAdminStatePut(chatId, JSON.stringify({ action: 'awaiting_input', key: keyOrAction }), env);
            
            let prompt = "";
            switch (keyOrAction) {
                case 'welcome_msg': prompt = "请发送**新的欢迎消息**："; break;
                case 'verif_q': prompt = "请发送**新的人机验证问题**："; break;
                case 'verif_a': prompt = "请发送**新的验证答案**："; break;
                case 'block_threshold': prompt = "请发送**屏蔽次数阈值** (纯数字)："; break;
                case 'backup_group_id': prompt = "请发送**新的备份群组 ID 或用户名**："; break; 
                case 'authorized_admins': prompt = "请发送**新的协管员 ID 或用户名列表**，多个请用逗号分隔 (例如：12345678, @username, 98765432)："; break;
                default: return;
            }
            
            const cancelBtn = { inline_keyboard: [[{ text: "❌ 取消编辑", callback_data: "config:menu" }]] };

            await telegramApi(env.BOT_TOKEN, "editMessageText", {
                chat_id: chatId,
                message_id: message.message_id,
                text: `${prompt}\n\n发送 \`/cancel\` 或点击下方按钮取消。`,
                parse_mode: "Markdown",
                reply_markup: cancelBtn,
            });
        } else if (actionType === 'add' && keyOrAction) {
            
            const newKey = keyOrAction + '_add';
            await dbAdminStatePut(chatId, JSON.stringify({ action: 'awaiting_input', key: newKey }), env);
            
            let prompt = "";
            let cancelBack = "";
            if (keyOrAction === 'keyword_responses') {
                 prompt = "请发送**新的自动回复规则**：\n\n**格式：** <code>关键词表达式===回复内容</code>\n\n例如：<code>你好|hello===欢迎您，请问有什么可以帮助您的？</code>";
                 cancelBack = "config:menu:autoreply";
            } else if (keyOrAction === 'block_keywords') {
                 prompt = "请发送**新的屏蔽关键词表达式**：\n\n**格式：** <code>关键词表达式</code>\n\n（支持正则表达式，例如：<code>(\uD83D\uDC49|\uD83D\uDCA3)</code>）";
                 cancelBack = "config:menu:keyword";
            } else {
                return;
            }

            const cancelBtn = { inline_keyboard: [[{ text: "❌ 取消添加", callback_data: cancelBack }]] };

            await telegramApi(env.BOT_TOKEN, "editMessageText", {
                chat_id: chatId,
                message_id: message.message_id,
                text: `${prompt}\n\n发送 \`/cancel\` 或点击下方按钮取消。`,
                parse_mode: "HTML",
                reply_markup: cancelBtn,
            });
        } else if (actionType === 'list' && keyOrAction) {
            await handleAdminRuleList(chatId, message.message_id, env, keyOrAction);
        } else if (actionType === 'delete' && keyOrAction && value) {
            await handleAdminRuleDelete(chatId, message.message_id, env, keyOrAction, value);
        }
        return; 
    }

    if (message.chat.id.toString() !== env.ADMIN_GROUP_ID) {
        return; 
    }

    const [action, userId] = data.split(':');

    if (action === 'pin_card') {
        await handlePinCard(callbackQuery, message, env);
        return;
    }

    await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", {
        callback_query_id: callbackQuery.id,
        text: `执行动作: ${action === 'block' ? '屏蔽' : '解除屏蔽'}...`,
        show_alert: false 
    });

    if (action === 'block') {
        await handleBlockUser(userId, message, env);
    } else if (action === 'unblock') {
        await handleUnblockUser(userId, message, env);
    }
}

async function handleBlockUser(userId, message, env) {
    try {
        await dbUserUpdate(userId, { is_blocked: true }, env);
        
        const userData = await dbUserGetOrCreate(userId, env);
        const userName = userData.user_info ? userData.user_info.name : `User ${userId}`;
        
        const newMarkup = getInfoCardButtons(userId, true);
        await telegramApi(env.BOT_TOKEN, "editMessageReplyMarkup", {
            chat_id: message.chat.id,
            message_id: message.message_id,
            reply_markup: newMarkup,
        });
        
        const confirmation = `❌ **用户 [${userName}] 已被屏蔽。**\n机器人将不再接收此人消息。`;
        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: message.chat.id,
            text: confirmation,
            message_thread_id: message.message_thread_id,
            parse_mode: "Markdown",
        });
        
    } catch (e) {
        console.error("处理屏蔽操作失败:", e.message);
    }
}

async function handleUnblockUser(userId, message, env) {
    try {
        await dbUserUpdate(userId, { is_blocked: false, block_count: 0 }, env);
        
        const userData = await dbUserGetOrCreate(userId, env);
        const userName = userData.user_info ? userData.user_info.name : `User ${userId}`;
        
        const newMarkup = getInfoCardButtons(userId, false);
        await telegramApi(env.BOT_TOKEN, "editMessageReplyMarkup", {
            chat_id: message.chat.id,
            message_id: message.message_id,
            reply_markup: newMarkup,
        });

        const confirmation = `✅ **用户 [${userName}] 已解除屏蔽。**\n机器人现在可以正常接收其消息。`;
        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: message.chat.id,
            text: confirmation,
            message_thread_id: message.message_thread_id,
            parse_mode: "Markdown",
        });

    } catch (e) {
        console.error("处理解除屏蔽操作失败:", e.message);
    }
}


async function handleAdminReply(message, env) {
    if (!message.is_topic_message || !message.message_thread_id) return;

    const adminGroupIdStr = env.ADMIN_GROUP_ID.toString();
    if (message.chat.id.toString() !== adminGroupIdStr) return;

    if (message.from && message.from.is_bot) return;

    const senderId = message.from.id.toString();
    const isAuthorizedAdmin = await isAdminUser(senderId, env);
    
    if (!isAuthorizedAdmin) {
        return; 
    }

    const topicId = message.message_thread_id.toString();
    const userId = await dbTopicUserGet(topicId, env);
    if (!userId) return;

    try {
        await telegramApi(env.BOT_TOKEN, "copyMessage", {
            chat_id: userId,
            from_chat_id: message.chat.id,
            message_id: message.message_id,
        });

    } catch (e) {
        console.error("handleAdminReply: copyMessage failed, attempting fallback:", e?.message || e);

        try {
            if (message.text) {
                 await telegramApi(env.BOT_TOKEN, "sendMessage", {
                    chat_id: userId,
                    text: message.text,
                });
            } else if (message.photo && message.photo.length) {
                const fileId = message.photo[message.photo.length - 1].file_id;
                await telegramApi(env.BOT_TOKEN, "sendPhoto", {
                    chat_id: userId,
                    photo: fileId,
                    caption: message.caption || "",
                });
            } else if (message.document) {
                await telegramApi(env.BOT_TOKEN, "sendDocument", {
                    chat_id: userId,
                    document: message.document.file_id,
                    caption: message.caption || "",
                });
            } else if (message.video) {
                await telegramApi(env.BOT_TOKEN, "sendVideo", {
                    chat_id: userId,
                    video: message.video.file_id,
                    caption: message.caption || "",
                });
            } else if (message.audio) {
                await telegramApi(env.BOT_TOKEN, "sendAudio", {
                    chat_id: userId,
                    audio: message.audio.file_id,
                    caption: message.caption || "",
                });
            } else if (message.voice) {
                await telegramApi(env.BOT_TOKEN, "sendVoice", {
                    chat_id: userId,
                    voice: message.voice.file_id,
                    caption: message.caption || "",
                });
            } else if (message.sticker) {
                await telegramApi(env.BOT_TOKEN, "sendSticker", {
                    chat_id: userId,
                    sticker: message.sticker.file_id,
                });
            } else if (message.animation) {
                await telegramApi(env.BOT_TOKEN, "sendAnimation", {
                    chat_id: userId,
                    animation: message.animation.file_id,
                    caption: message.caption || "",
                });
            } else {
                await telegramApi(env.BOT_TOKEN, "sendMessage", {
                    chat_id: userId,
                    text: "管理员发送了机器人无法直接转发的内容（例如投票或某些特殊媒体）。",
                });
            }
        } catch (e2) {
            console.error("handleAdminReply fallback also failed:", e2?.message || e2);
            return; 
        }
    }
    
    const enableAdminReceipt = (await getConfig('enable_admin_receipt', env, 'true')).toLowerCase() === 'true';

    if (enableAdminReceipt) {
        const userData = await dbUserGetOrCreate(userId, env);
        let confirmationDetail;

        if (userData.user_info && userData.user_info.username && userData.user_info.username !== '无') {
            const safeUsername = escapeHtml(userData.user_info.username);
            confirmationDetail = `用户名: <a href="tg://user?id=${userId}">${safeUsername}</a>`;
        } else {
            confirmationDetail = `ID: <code>${userId}</code>`;
        }

        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: message.chat.id,
            message_thread_id: message.message_thread_id,
            text: `✅ 回复已发送给用户 (${confirmationDetail})`,
            parse_mode: "HTML",
            reply_to_message_id: message.message_id,
            disable_notification: true,
        }).catch(e => {}); 
    }
}
