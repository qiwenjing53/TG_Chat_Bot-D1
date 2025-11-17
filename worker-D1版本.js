/**
 * Telegram 双向机器人 Cloudflare Worker (D1 版本)
 * * [集成 Cloudflare Turnstile]
 * - 新增 Cloudflare Turnstile 作为第一道人机验证防线。
 * - 机器人现在是一个路由器，处理:
 * - POST / (或 /<BOT_TOKEN>): Telegram Webhook (原功能)
 * - GET /verify: 提供给用户的 Turnstile 验证网页
 * - POST /submit_token: 接收来自网页的 Turnstile 令牌并进行验证
 * - 引入了新的用户状态 'pending_turnstile'。
 * - 重写了 handleStart 和 handlePrivateMessage 的状态机逻辑。
 * * [部署要求 - 新增]
 * 3. 环境变量必须额外添加:
 * - WORKER_URL: Worker 的完整 URL (例如 https://my-worker.example.workers.dev)
 * - TURNSTILE_SITE_KEY: Cloudflare Turnstile 站点密钥
 * - TURNSTILE_SECRET_KEY: Cloudflare Turnstile 密钥
 * * * [更新日志 - 回执控制]
 * - 在“过滤与系统功能”菜单中增加了“管理员回复回执”开关。
 * - 管理员现在可以关闭回复用户时的自动确认消息。
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
        // console.log("D1 Migration successful/already complete.");
    } catch (e) {
        console.error("D1 Migration Failed:", e);
        throw new Error(`D1 Initialization Failed: ${e.message}`);
    }
}


// --- 辅助函数 ---

function escapeHtml(text) {
  if (!text) return '';
  // Cloudflare Worker 不支持 String.prototype.replaceAll, 使用全局替换
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
    
    // 将用户名显示为可点击链接
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

/**
 * 生成用户资料卡下方的操作按钮（屏蔽/解禁/置顶）
 */
function getInfoCardButtons(userId, isBlocked) {
    const blockAction = isBlocked ? "unblock" : "block";
    const blockText = isBlocked ? "✅ 解除屏蔽 (Unblock)" : "🚫 屏蔽此人 (Block)";
    return {
        inline_keyboard: [
            [{ // Row 1: Block/Unblock Button
                text: blockText,
                callback_data: `${blockAction}:${userId}`
            }],
            [{ // Row 2: Pin Button
                text: "📌 置顶此消息 (Pin Card)",
                callback_data: `pin_card:${userId}` 
            }]
        ]
    };
}


/**
 * 优先从 D1 获取配置，其次从环境变量获取，最后使用默认值。
 */
async function getConfig(key, env, defaultValue) {
    const configValue = await dbConfigGet(key, env);
    
    // 如果 D1 中有配置，直接返回 D1 的值
    if (configValue !== null) {
        return configValue;
    }
    
    // 如果 D1 中没有，检查环境变量（作为后备或兼容性）
    const envKey = key.toUpperCase()
                      .replace('WELCOME_MSG', 'WELCOME_MESSAGE')
                      .replace('VERIF_Q', 'VERIFICATION_QUESTION')
                      .replace('VERIF_A', 'VERIFICATION_ANSWER')
                      .replace(/_FORWARDING/g, '_FORWARDING');
    
    const envValue = env[envKey];
    if (envValue !== undefined && envValue !== null) {
        return envValue;
    }
    
    // 都没有，返回代码默认值
    return defaultValue;
}

/**
 * 检查用户是否是主管理员 (来自 ADMIN_IDS 环境变量)
 */
function isPrimaryAdmin(userId, env) {
    if (!env.ADMIN_IDS) return false;
    // 确保 ADMIN_IDS 是逗号分隔的字符串
    const adminIds = env.ADMIN_IDS.split(',').map(id => id.trim());
    return adminIds.includes(userId.toString());
}


/**
 * 获取授权协管员 ID 列表
 */
async function getAuthorizedAdmins(env) {
    const jsonString = await getConfig('authorized_admins', env, '[]');
    try {
        const adminList = JSON.parse(jsonString);
        // 确保列表是有效的数组，并且所有元素都被修剪并转换为字符串
        return Array.isArray(adminList) ? adminList.map(id => id.toString().trim()).filter(id => id !== "") : [];
    } catch (e) {
        console.error("Failed to parse authorized_admins from D1:", e);
        return [];
    }
}

/**
 * 检查用户是否是任意管理员 (主管理员或授权协管员)
 */
async function isAdminUser(userId, env) {
    // 1. 检查是否是主管理员 (ADMIN_IDS 环境变量)
    if (isPrimaryAdmin(userId, env)) {
        return true;
    }

    // 2. 检查是否是授权协管员 (D1 配置)
    const authorizedAdmins = await getAuthorizedAdmins(env);
    return authorizedAdmins.includes(userId.toString());
}


// --- 规则管理重构区域 ---

/**
 * 获取自动回复规则列表（从 JSON 字符串解析为数组）
 * 结构：[{ keywords: "a|b", response: "reply", id: timestamp }, ...]
 */
async function getAutoReplyRules(env) {
    // 尝试从 D1 获取配置，默认值是空数组的 JSON 字符串
    const jsonString = await getConfig('keyword_responses', env, '[]');
    try {
        const rules = JSON.parse(jsonString);
        return Array.isArray(rules) ? rules : [];
    } catch (e) {
        console.error("Failed to parse keyword_responses from D1:", e);
        return [];
    }
}

/**
 * 获取屏蔽关键词列表（从 JSON 字符串解析为数组）
 * 结构：["keyword1|keyword2", "keyword3", ...]
 */
async function getBlockKeywords(env) {
    // 尝试从 D1 获取配置，默认值是空数组的 JSON 字符串
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
        // 捕获 API 错误，用于话题不存在等场景
        // console.error(`Telegram API error (${methodName}): ${data.description}. Params: ${JSON.stringify(params)}`);
        throw new Error(`${methodName} failed: ${data.description || JSON.stringify(data)}`);
    }

    return data.result;
}


// --- [新增] Cloudflare Turnstile 验证辅助函数 ---

/**
 * [新增] 验证 Turnstile 令牌
 * @param {string} token - 来自客户端的 cf-turnstile-response 令牌
 * @param {object} env - Worker 环境变量
 */
async function validateTurnstile(token, env) {
    if (!token) {
        return false;
    }
    if (!env.TURNSTILE_SECRET_KEY) {
        console.error("Turnstile validation failed: TURNSTILE_SECRET_KEY is not set.");
        return false;
    }

    try {
        const response = await fetch('https://challenges.cloudflare.com/turnstile/v0/siteverify', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                secret: env.TURNSTILE_SECRET_KEY,
                response: token,
                // remoteip: request.headers.get('CF-Connecting-IP'), // 可选
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
 * [新增] 处理对 /verify 路径的 GET 请求，返回 Turnstile 验证网页
 */
async function handleVerificationPage(request, env) {
    const url = new URL(request.url);
    const userId = url.searchParams.get('user_id');

    if (!userId) {
        return new Response("Missing user_id parameter.", { status: 400 });
    }

    if (!env.TURNSTILE_SITE_KEY) {
        console.error("handleVerificationPage: TURNSTILE_SITE_KEY is not set.");
        return new Response("Bot configuration error (missing site key).", { status: 500 });
    }

    const html = `
    <!DOCTYPE html>
    <html lang="zh-CN">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>人机验证</title>
        <script src="https://challenges.cloudflare.com/turnstile/v0/api.js" async defer></script>
        <style>
            body { display: flex; flex-direction: column; justify-content: center; align-items: center; min-height: 100vh; font-family: sans-serif; background: #f4f4f4; color: #333; }
            #container { background: #fff; padding: 2rem; border-radius: 8px; box-shadow: 0 4px 12px rgba(0,0,0,0.1); text-align: center; }
            h1 { margin-top: 0; }
            #message { margin-top: 1.5rem; font-size: 1.1rem; font-weight: bold; }
            #message.success { color: green; }
            #message.error { color: red; }
            .cf-turnstile { margin-top: 1.5rem; }
        </style>
    </head>
    <body>
        <div id="container">
            <h1>请完成人机验证</h1>
            <p>请点击下方的复选框以证明您是人类。</p>
            
            <div class="cf-turnstile" 
                 data-sitekey="${env.TURNSTILE_SITE_KEY}" 
                 data-callback="onTurnstileSuccess"
                 data-expired-callback="onTurnstileExpired"
                 data-error-callback="onTurnstileError">
            </div>

            <div id="message"></div>
        </div>

        <script>
            const userId = "${userId}"; // 将 userId 嵌入到 JS
            const messageEl = document.getElementById('message');

            function onTurnstileSuccess(token) {
                messageEl.textContent = '验证成功，正在提交...';
                messageEl.className = '';

                // 将令牌和 userId 发送到 Worker 的 /submit_token 端点
                fetch('/submit_token', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ token: token, userId: userId })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        messageEl.textContent = '✅ 验证通过！您现在可以返回 Telegram 回答下一个问题了。';
                        messageEl.className = 'success';
                        // 验证成功后，可以尝试关闭窗口或提示用户返回
                        setTimeout(() => {
                           try { window.close(); } catch(e) { console.warn('Could not close window.'); }
                        }, 3000);
                    } else {
                        messageEl.textContent = '❌ 令牌提交失败，请刷新页面重试。';
                        messageEl.className = 'error';
                    }
                })
                .catch(err => {
                    console.error('Submit error:', err);
                    messageEl.textContent = '❌ 验证提交时发生网络错误。';
                    messageEl.className = 'error';
                });
            }
            
            function onTurnstileExpired() {
                messageEl.textContent = '验证已过期，请重试。';
                messageEl.className = 'error';
            }
            
            function onTurnstileError() {
                 messageEl.textContent = '验证加载失败，请刷新或检查网络。';
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

/**
 * [新增] 处理 /submit_token 路径的 POST 请求，验证 Turnstile 令牌
 */
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
        // 将状态从 'pending_turnstile' 更新为 'pending_verification' (等待L2问题答案)
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

        // 4. 向网页返回成功响应
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

/**
 * [修改] Worker 入口：重构为路由器
 * 1. 运行 D1 迁移
 * 2. 检查 Turnstile 环境变量
 * 3. 路由请求:
 * - GET /verify -> handleVerificationPage()
 * - POST /submit_token -> handleSubmitToken()
 * - POST / (或 /<TOKEN>) -> handleUpdate() (Telegram Webhook)
 */
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
          // 注意：如果环境变量缺失，Turnstile 验证流程将失败
      }
      
      const url = new URL(request.url);

      // 3. 路由
      try {
          // 路由 A: 网页验证页面 (GET /verify?user_id=...)
          if (request.method === "GET" && url.pathname === "/verify") {
              return handleVerificationPage(request, env);
          }
          
          // 路由 B: 网页提交 Turnstile 令牌 (POST /submit_token)
          if (request.method === "POST" && url.pathname === "/submit_token") {
              return handleSubmitToken(request, env);
          }

          // 路由 C: Telegram Webhook (POST /)
          // 假设 webhook 设置在根目录 "/" 或 包含 Token 的路径
          if (request.method === "POST") {
              try {
                  const update = await request.json();
                  ctx.waitUntil(handleUpdate(update, env)); // 异步处理 Webhook
                  return new Response("OK"); // 立即响应 Telegram
              } catch (e) {
                  console.error("Failed to parse Telegram update:", e);
                  return new Response("Invalid JSON", { status: 400 });
              }
          }

          // 兜底 GET /
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

/**
 * 处理 Telegram 的 Webhook 更新 (由 fetch 路由器调用)
 */
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

/**
 * [修改] 处理私聊消息 (状态机重构)
 */
async function handlePrivateMessage(message, env) {
    const chatId = message.chat.id.toString();
    const text = message.text || "";
    const userId = chatId;

    // 检查是否是主管理员 (只有主管理员能访问配置菜单)
    const isPrimary = isPrimaryAdmin(userId, env);
    // 检查是否是任意管理员 (主管理员或授权协管员)
    const isAdmin = await isAdminUser(userId, env);
    
    // 1. 检查 /start 或 /help 命令
    if (text === "/start" || text === "/help") {
        if (isPrimary) { // 只有主管理员能访问配置菜单
            await handleAdminConfigStart(chatId, env);
        } else {
            // [MODIFIED] 调用新的 handleStart 逻辑，它会处理 L1 和 L2 验证
            await handleStart(chatId, env);
        }
        return;
    }
    
    // 从 D1 获取用户数据
    const user = await dbUserGetOrCreate(userId, env);
    const isBlocked = user.is_blocked;

    if (isBlocked) {
        return; 
    }
    
    // 主管理员在配置编辑状态中发送的文本输入 (此逻辑块保持不变)
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
    
    // 协管员绕过验证逻辑 (此逻辑块保持不变)
    if (isAdmin && user.user_state !== "verified") {
        user.user_state = "verified"; 
        await dbUserUpdate(userId, { user_state: "verified" }, env); 
    }

    // 2. 检查用户的验证状态
    // [MODIFIED] 新的验证状态机
    const userState = user.user_state;

    if (userState === "new" || userState === "pending_turnstile") {
        // 用户未通过L1验证，且发送的不是 /start
        // 提示他们使用 /start 或点击按钮
        
        if (userState === "pending_turnstile" && env.WORKER_URL) {
            // 友好地重新发送按钮
            const workerUrl = env.WORKER_URL.replace(/\/$/, '');
            const verificationUrl = `${workerUrl}/verify?user_id=${chatId}`;
            const keyboard = { inline_keyboard: [[{ text: "➡️ 点击进行人机验证", url: verificationUrl }]] };
            await telegramApi(env.BOT_TOKEN, "sendMessage", { 
                chat_id: chatId, 
                text: "请先点击按钮完成第一道 Cloudflare 人机验证。", 
                reply_markup: keyboard 
            });
        } else {
             // 状态为 'new' 或 环境变量缺失
             await telegramApi(env.BOT_TOKEN, "sendMessage", { 
                 chat_id: chatId, 
                 text: "请使用 /start 命令开始验证流程。" 
             });
        }
        return; // 阻止后续操作

    } else if (userState === "pending_verification") {
        // 用户已通过L1，正在回答L2问题
        await handleVerification(chatId, text, env);
        return; // 阻止后续操作
    
    } else if (userState === "verified") {
        // [UNCHANGED] 已验证通过，进入原有的转发逻辑
        
        // --- 首次消息纯文本检查 ---
        if (!user.first_message_sent) { 
            // [修复漏洞] 增强对“纯文本”的判断，拒绝任何带实体（链接、加粗、@ 等）的消息
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
                return; // 阻止非纯文本的首次消息
            }
            // 如果是纯文本，则正常向下执行，状态将在成功发送后更新
        }

        // --- [关键词屏蔽检查] ---
        const blockKeywords = await getBlockKeywords(env); // 获取 JSON 数组
        const blockThreshold = parseInt(await getConfig('block_threshold', env, "5"), 10) || 5; 
        
        if (blockKeywords.length > 0 && text) { 
            let currentCount = user.block_count;
            
            for (const keyword of blockKeywords) {
                try {
                    // 使用新结构中的字符串构建 RegExp
                    const regex = new RegExp(keyword, 'gi'); 
                    if (regex.test(text)) {
                        currentCount += 1;
                        
                        // 更新 D1 中的屏蔽计数
                        await dbUserUpdate(userId, { block_count: currentCount }, env);
                        
                        const blockNotification = `⚠️ 您的消息触发了屏蔽关键词过滤器 (${currentCount}/${blockThreshold}次)，此消息已被丢弃，不会转发给对方。`;
                        
                        if (currentCount >= blockThreshold) {
                            // 达到阈值，自动屏蔽用户 (is_blocked = 1)
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
                } catch(e) {
                    console.error("Invalid keyword block regex:", keyword, e);
                    // 忽略无效的正则，继续检查下一个
                }
            }
        }

        // --- [转发内容过滤检查] ---
        const filters = {
            // 图片/视频/文件 (原 enable_image_forwarding)
            media: (await getConfig('enable_image_forwarding', env, 'true')).toLowerCase() === 'true',
            // 链接
            link: (await getConfig('enable_link_forwarding', env, 'true')).toLowerCase() === 'true',
            // 纯文本
            text: (await getConfig('enable_text_forwarding', env, 'true')).toLowerCase() === 'true',
            // 频道转发 (细分)
            channel_forward: (await getConfig('enable_channel_forwarding', env, 'true')).toLowerCase() === 'true', 
            
            // 新增过滤器
            // 任何转发消息 (用户/群组/频道)
            any_forward: (await getConfig('enable_forward_forwarding', env, 'true')).toLowerCase() === 'true', 
            // 音频文件和语音消息
            audio_voice: (await getConfig('enable_audio_forwarding', env, 'true')).toLowerCase() === 'true', 
            // 贴纸，emojy，gif (sticker, animation)
            sticker_gif: (await getConfig('enable_sticker_forwarding', env, 'true')).toLowerCase() === 'true', 
        };

        let isForwardable = true;
        let filterReason = '';

        const hasLinks = (msg) => {
            const entities = msg.entities || msg.caption_entities || [];
            return entities.some(entity => entity.type === 'url' || entity.type === 'text_link');
        };

        // 1. 任何转发消息（用户、群组、频道）
        if (message.forward_from || message.forward_from_chat) {
             // 检查总开关
             if (!filters.any_forward) {
                isForwardable = false;
                filterReason = '转发消息 (来自用户/群组/频道)';
            } 
            // 如果总开关允许，但它是频道转发，再检查频道细分开关
            else if (message.forward_from_chat && message.forward_from_chat.type === 'channel' && !filters.channel_forward) {
                isForwardable = false;
                filterReason = '频道转发消息';
            }
        } 
        // 2. 音频文件和语音消息
        else if (message.audio || message.voice) {
            if (!filters.audio_voice) {
                isForwardable = false;
                filterReason = '音频或语音消息';
            }
        }
        // 3. 贴纸，emojy，gif (sticker, animation)
        else if (message.sticker || message.animation) {
             if (!filters.sticker_gif) {
                isForwardable = false;
                filterReason = '贴纸或GIF';
            }
        }
        // 4. 其他媒体（Photo, Video, Document） - 使用 'media' (原 enable_image_forwarding)
        else if (message.photo || message.video || message.document) {
            if (!filters.media) {
                isForwardable = false;
                filterReason = '媒体内容（图片/视频/文件）';
            }
        } 
        
        // 5. 链接检查 (保留原逻辑，作用于任何包含链接的消息)
        if (isForwardable && hasLinks(message)) {
            if (!filters.link) {
                isForwardable = false;
                filterReason = filterReason ? `${filterReason} (并包含链接)` : '包含链接的内容';
            }
        }

        // 6. 纯文本检查 (保留原逻辑)
        // 检查是否是纯文本（排除所有媒体和转发类型）
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
        
        // --- [Keyword Auto-Reply Check] ---
        const autoResponseRules = await getAutoReplyRules(env); // 获取 JSON 数组
        if (autoResponseRules.length > 0 && text) { 
            
            for (const rule of autoResponseRules) {
                try {
                    // 使用新结构中的 keywords 字符串构建 RegExp
                    const regex = new RegExp(rule.keywords, 'gi'); 
                    if (regex.test(text)) {
                        const autoReplyPrefix = "此消息为自动回复\n\n";
                        await telegramApi(env.BOT_TOKEN, "sendMessage", {
                            chat_id: chatId,
                            text: autoReplyPrefix + rule.response,
                        });
                        return; 
                    }
                } catch(e) {
                    console.error("Invalid auto-reply regex:", rule.keywords, e);
                    // 忽略无效的正则，继续检查下一个
                }
            }
        }
        
        await handleRelayToTopic(message, user, env); // 传递 user 对象
        
    } else {
        // 兜底状态，理论上不应到达
        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: chatId,
            text: "您的状态异常，请使用 /start 命令重试。",
        });
    }
}

// --- 验证逻辑 (使用 D1) ---

/**
 * [修改] L1 (Turnstile) 和 L2 (问题) 验证流程的入口
 * 此函数现在处理 /start 命令，并根据用户当前状态引导他们
 */
async function handleStart(chatId, env) {
    const user = await dbUserGetOrCreate(chatId, env);
    
    // 检查 Turnstile 必需的环境变量
    if (!env.WORKER_URL || !env.TURNSTILE_SITE_KEY) {
        await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: chatId, text: "⚠️ 机器人配置错误：Cloudflare Turnstile 未正确配置。请联系管理员。" });
        console.error("handleStart: Missing WORKER_URL or TURNSTILE_SITE_KEY");
        return;
    }

    // 根据用户状态决定操作
    switch (user.user_state) {
        case 'new':
        case 'pending_turnstile':
            // 状态为 'new' 或 'pending_turnstile'，发送L1验证
            const workerUrl = env.WORKER_URL.replace(/\/$/, ''); // 移除末尾斜杠
            const verificationUrl = `${workerUrl}/verify?user_id=${chatId}`;
            const welcomeMessage = await getConfig('welcome_msg', env, "欢迎！在使用之前，请先完成人机验证。");

            const text = welcomeMessage + "\n\n请点击下方按钮，完成第一道 Cloudflare 人机验证。";
            const keyboard = { inline_keyboard: [[{ text: "➡️ 点击进行人机验证", url: verificationUrl }]] };

            await telegramApi(env.BOT_TOKEN, "sendMessage", {
                chat_id: chatId,
                text: text,
                reply_markup: keyboard
            });
            
            // 更新状态
            if (user.user_state === 'new') {
                 await dbUserUpdate(chatId, { user_state: "pending_turnstile" }, env);
            }
            break;
        
        case 'pending_verification':
            // 已通过L1，等待L2答案，重新发送L2问题
            const defaultVerifQ = "问题：1+1=?\n\n提示：\n1. 正确答案不是“2”。\n2. 答案在机器人简介内，请看简介的答案进行回答。";
            const verificationQuestion = await getConfig('verif_q', env, defaultVerifQ);
            
            await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: chatId, text: "您已通过第一道验证。请回答以下第二道问题：\n\n" + verificationQuestion });
            break;

        case 'verified':
            // 已完全验证
            await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: chatId, text: "您已通过验证，可以正常发送消息。" });
            break;
    }
}

/**
 * [未修改] L2 验证：处理问题答案
 */
async function handleVerification(chatId, answer, env) {
    const expectedAnswer = await getConfig('verif_a', env, "3"); 

    if (answer.trim() === expectedAnswer.trim()) {
        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: chatId,
            text: "✅ L2 验证通过！您现在可以发送消息了。\n\n**注意：您的第一条消息必须是纯文本内容。**",
            parse_mode: "Markdown",
        });
        // 更新 D1 中的用户状态, first_message_sent 保持默认的 0/false
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
        // 非主管理员不显示配置菜单
        await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: chatId, text: "您是授权协管员，已绕过验证。此菜单仅供主管理员使用。", });
        return;
    }
    
    const menuText = `
⚙️ <b>机器人主配置菜单</b>

请选择要管理的配置类别：
    `.trim();

    const menuKeyboard = {
        inline_keyboard: [
            // 第一行：配置
            [{ text: "📝 基础配置 (验证问答)", callback_data: "config:menu:base" }],
            // 第二行：功能
            [{ text: "🤖 自动回复管理", callback_data: "config:menu:autoreply" }],
            [{ text: "🚫 关键词屏蔽管理", callback_data: "config:menu:keyword" }],
            // 第三行：过滤与系统设置 (修改了名称)
            [{ text: "🛠 过滤与系统功能", callback_data: "config:menu:filter" }],
            // 协管员授权设置按钮
            [{ text: "🧑‍💻 协管员授权设置", callback_data: "config:menu:authorized" }], 
            // 备份群组设置按钮
            [{ text: "💾 备份群组设置", callback_data: "config:menu:backup" }], 
            // 第四行：刷新
            [{ text: "🔄 刷新主菜单", callback_data: "config:menu" }],
        ]
    };

    // 清除任何未完成的编辑状态
    await dbAdminStateDelete(chatId, env);

    // 检查是否是编辑旧消息的回调（从其他子菜单返回）
    if (env.last_config_message_id) {
        await telegramApi(env.BOT_TOKEN, "editMessageText", {
            chat_id: chatId,
            message_id: env.last_config_message_id,
            text: menuText,
            parse_mode: "HTML",
            reply_markup: menuKeyboard,
        }).catch(e => console.error("尝试编辑旧菜单失败:", e.message)); // 忽略编辑失败
        return;
    }


    await telegramApi(env.BOT_TOKEN, "sendMessage", {
        chat_id: chatId,
        text: menuText,
        parse_mode: "HTML",
        reply_markup: menuKeyboard,
    });
}

/**
 * 基础配置子菜单 - 兼容编辑和发送新消息
 */
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

/**
 * 协管员授权设置子菜单
 */
async function handleAdminAuthorizedConfigMenu(chatId, messageId, env) {
    const primaryAdmins = env.ADMIN_IDS ? env.ADMIN_IDS.split(',').map(id => id.trim()).filter(id => id !== "") : [];
    const authorizedAdmins = await getAuthorizedAdmins(env);
    
    const allAdmins = [...new Set([...primaryAdmins, ...authorizedAdmins])]; // 合并并去重
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

/**
 * 自动回复子菜单 - 兼容编辑和发送新消息
 */
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

/**
 * 关键词屏蔽子菜单 - 兼容编辑和发送新消息
 */
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

/**
 * 备份群组设置子菜单 - 兼容编辑和发送新消息
 */
async function handleAdminBackupConfigMenu(chatId, messageId, env) {
    // 备份群组 ID 存储在 'backup_group_id' 键中
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


/**
 * 规则列表和删除界面
 */
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
                // 自动回复规则：使用 ID 进行删除
                const keywordsSnippet = rule.keywords.substring(0, 15);
                const responseSnippet = rule.response.substring(0, 20);
                label = `${index + 1}. <code>${escapeHtml(keywordsSnippet)}...</code> ➡️ ${escapeHtml(responseSnippet)}...`;
                deleteId = rule.id;
            } else if (key === 'block_keywords') {
                // 屏蔽关键词：直接使用关键词字符串作为 ID (确保唯一)
                const keywordSnippet = rule.substring(0, 25);
                label = `${index + 1}. <code>${escapeHtml(keywordSnippet)}...</code>`;
                deleteId = rule; 
            }
            
            // 添加列表信息到文本
            menuText += `\n${label}`;

            // 添加删除按钮
            ruleButtons.push([
                { 
                    text: `🗑️ 删除 ${index + 1}`, 
                    // config:delete:key:id
                    callback_data: `config:delete:${key}:${deleteId}` 
                }
            ]);
        });
    }

    // 底部返回按钮
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

/**
 * 规则删除逻辑
 */
async function handleAdminRuleDelete(chatId, messageId, env, key, id) {
    let rules = [];
    let typeName = "";
    let backCallback = "";

    if (key === 'keyword_responses') {
        rules = await getAutoReplyRules(env);
        typeName = "自动回复规则";
        backCallback = "config:menu:autoreply";
        // 自动回复规则使用 ID 删除
        rules = rules.filter(rule => rule.id.toString() !== id.toString());
    } else if (key === 'block_keywords') {
        rules = await getBlockKeywords(env);
        typeName = "屏蔽关键词";
        backCallback = "config:menu:keyword";
        // 屏蔽关键词直接使用字符串删除
        rules = rules.filter(keyword => keyword !== id);
    } else {
        return;
    }

    // 存储更新后的规则列表
    await dbConfigPut(key, JSON.stringify(rules), env);

    // 刷新列表界面
    await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: chatId, text: `✅ ${typeName}已删除并更新。`, show_alert: false });
    await handleAdminRuleList(chatId, messageId, env, key);
}


/**
 * [修改] 按类型过滤与系统设置子菜单 - 兼容编辑和发送新消息
 */
async function handleAdminTypeBlockMenu(chatId, messageId, env) {
    // 获取当前状态，检查 D1 -> ENV -> 默认值 'true'
    const mediaStatus = (await getConfig('enable_image_forwarding', env, 'true')).toLowerCase() === 'true'; // 图片/视频/文件
    const linkStatus = (await getConfig('enable_link_forwarding', env, 'true')).toLowerCase() === 'true';
    const textStatus = (await getConfig('enable_text_forwarding', env, 'true')).toLowerCase() === 'true';
    
    const channelForwardStatus = (await getConfig('enable_channel_forwarding', env, 'true')).toLowerCase() === 'true'; // 频道转发
    const anyForwardStatus = (await getConfig('enable_forward_forwarding', env, 'true')).toLowerCase() === 'true'; // 任何转发
    const audioVoiceStatus = (await getConfig('enable_audio_forwarding', env, 'true')).toLowerCase() === 'true'; // 音频/语音
    const stickerGifStatus = (await getConfig('enable_sticker_forwarding', env, 'true')).toLowerCase() === 'true'; // 贴纸/GIF

    // [新增] 管理员回复回执开关
    const adminReceiptStatus = (await getConfig('enable_admin_receipt', env, 'true')).toLowerCase() === 'true';

    const statusToText = (status) => status ? "✅ 允许/开启" : "❌ 屏蔽/关闭";
    // 构造回调数据：config:toggle:key:new_value (e.g., config:toggle:enable_image_forwarding:false)
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
            // [新增] 系统功能
            [{ text: `管理员回复回执: ${statusToText(adminReceiptStatus)}`, callback_data: statusToCallback('enable_admin_receipt', adminReceiptStatus) }],
            
            // 新增的过滤类型
            [{ text: `转发消息 (用户/群组/频道): ${statusToText(anyForwardStatus)}`, callback_data: statusToCallback('enable_forward_forwarding', anyForwardStatus) }],
            [{ text: `音频/语音消息 (Audio/Voice): ${statusToText(audioVoiceStatus)}`, callback_data: statusToCallback('enable_audio_forwarding', audioVoiceStatus) }],
            [{ text: `贴纸/GIF (Sticker/Animation): ${statusToText(stickerGifStatus)}`, callback_data: statusToCallback('enable_sticker_forwarding', stickerGifStatus) }],
            
            // 现有的过滤类型
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
        // 删除状态
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
        // 备份群组 ID 仅移除首尾空格
        } else if (adminState.key === 'backup_group_id') {
            finalValue = text.trim();
        // 协管员授权列表处理
        } else if (adminState.key === 'authorized_admins') {
            // 将输入字符串按逗号分隔，并去除空格和空项，最终存储为 JSON 数组
            const adminList = text.split(',').map(id => id.trim()).filter(id => id !== "");
            finalValue = JSON.stringify(adminList); // 存储 JSON 字符串
            
        }

        // --- 新增规则逻辑 ---
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
            // 清除状态
            await dbAdminStateDelete(userId, env);
            await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: userId, text: successMsg, parse_mode: "HTML" });
            await handleAdminKeywordBlockMenu(userId, 0, env); 
            return;
        } else if (adminState.key === 'keyword_responses_add') {
            const rules = await getAutoReplyRules(env);
            
            // 格式: 关键词===回复内容
            const parts = finalValue.split('===');
            if (parts.length === 2 && parts[0].trim() && parts[1].trim()) {
                const newRule = {
                    keywords: parts[0].trim(),
                    response: parts[1].trim(),
                    id: Date.now(), // 使用时间戳作为唯一ID
                };
                rules.push(newRule);
                await dbConfigPut('keyword_responses', JSON.stringify(rules), env);
                successMsg = `✅ 自动回复规则已添加。关键词: <code>${escapeHtml(newRule.keywords)}</code>`;
            } else {
                 successMsg = `⚠️ 自动回复规则未添加。请确保格式正确：<code>关键词表达式===回复内容</code>`;
            }
            // 清除状态
            await dbAdminStateDelete(userId, env);
            await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: userId, text: successMsg, parse_mode: "HTML" });
            await handleAdminAutoReplyMenu(userId, 0, env); 
            return;
        }

        // --- 基础配置修改逻辑 ---
        
        // 存储新的配置到 D1 的 config 表
        await dbConfigPut(adminState.key, finalValue, env);
        
        // 清除管理员状态
        await dbAdminStateDelete(userId, env);
        
        switch (adminState.key) {
            case 'welcome_msg': successMsg = `✅ <b>欢迎消息</b>已更新。`; break;
            case 'verif_q': successMsg = `✅ <b>验证问题</b>已更新。`; break;
            case 'verif_a': successMsg = `✅ <b>验证答案</b>已更新为：<code>${escapeHtml(finalValue)}</code>`; break;
            case 'block_threshold': successMsg = `✅ <b>屏蔽次数阈值</b>已更新为：<code>${escapeHtml(finalValue)}</code>`; break;
            // 备份群组 ID 成功消息
            case 'backup_group_id': 
                if (finalValue === '') {
                    successMsg = `✅ <b>备份群组 ID</b>已清除，备份功能已禁用。`;
                } else {
                    successMsg = `✅ <b>备份群组 ID</b>已更新为：<code>${escapeHtml(finalValue)}</code>`; 
                }
                break;
            // 协管员授权列表成功消息
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

        // 发送成功消息
        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: userId,
            text: successMsg,
            parse_mode: "HTML",
        });

        // 跳转到对应的子菜单
        let nextMenuAction = 'config:menu';
        if (['welcome_msg', 'verif_q', 'verif_a'].includes(adminState.key)) {
            nextMenuAction = 'config:menu:base';
        } else if (adminState.key === 'block_threshold') {
            nextMenuAction = 'config:menu:keyword';
        // 备份群组 ID 菜单跳转
        } else if (adminState.key === 'backup_group_id') {
            nextMenuAction = 'config:menu:backup';
        // 协管员授权列表菜单跳转
        } else if (adminState.key === 'authorized_admins') {
            nextMenuAction = 'config:menu:authorized';
        }
        
        // 发送一个新的菜单消息，实现自动跳转。
        if (nextMenuAction === 'config:menu:base') {
            await handleAdminBaseConfigMenu(userId, 0, env); 
        } else if (nextMenuAction === 'config:menu:autoreply') {
             await handleAdminAutoReplyMenu(userId, 0, env); 
        } else if (nextMenuAction === 'config:menu:keyword') {
             await handleAdminKeywordBlockMenu(userId, 0, env); 
        // 备份群组 ID 菜单跳转
        } else if (nextMenuAction === 'config:menu:backup') {
             await handleAdminBackupConfigMenu(userId, 0, env); 
        // 协管员授权列表菜单跳转
        } else if (nextMenuAction === 'config:menu:authorized') {
             await handleAdminAuthorizedConfigMenu(userId, 0, env); 
        } else {
             await handleAdminConfigStart(userId, env); // 返回主菜单
        }


    } else {
        // 删除状态
        await dbAdminStateDelete(userId, env);
        // 此处错误提示已修复，不会出现 D1_ERROR:no such table:admin_state:SQLITE_ERROR
        await telegramApi(env.BOT_TOKEN, "sendMessage", { chat_id: userId, text: "⚠️ 状态错误，已重置。请重新使用 /start 访问菜单。", });
    }
}


async function handleRelayToTopic(message, user, env) { // 接收 user 对象
    const { from: userDetails, date } = message;
    const { userId, topicName, infoCard } = getUserInfo(userDetails, date);
    let topicId = user.topic_id;
    const isBlocked = user.is_blocked;

    // Helper: 创建新话题并发送信息卡
    const createTopicForUser = async () => {
        try {
            const newTopic = await telegramApi(env.BOT_TOKEN, "createForumTopic", {
                chat_id: env.ADMIN_GROUP_ID,
                name: topicName,
            });
            const newTopicId = newTopic.message_thread_id.toString();
            const { name, username } = getUserInfo(userDetails, date);
            const newInfo = { name, username, first_message_timestamp: date };

            // 存储 topic_id 和 user_info 到 D1
            await dbUserUpdate(userId, { 
                topic_id: newTopicId,
                user_info: newInfo // dbUserUpdate 会自动处理为 JSON
            }, env);

            // 发送信息卡到新话题，附带操作按钮
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

    // 如果没有 topicId，直接创建
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

    // 现在尝试把用户的消息复制到 topicId；如果失败（例如话题已被删除/无效），则重建话题后再转发
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
            // 捕获话题不存在的特定错误 (例如 Bad Request: message thread not found)
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
        // 出错：可能话题被删除或无效，清理 D1 并尝试重建话题一次
        try {
            // 删除旧的 topic_id 映射
            await dbUserUpdate(userId, { topic_id: null }, env);
            
            // 重新创建话题并把消息复制到新话题
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

    // --- 向用户发送消息送达回执 ---
    await telegramApi(env.BOT_TOKEN, "sendMessage", {
        chat_id: userId,
        text: "✅ 你的消息已发送给管理员，请耐心等待回复。",
        reply_to_message_id: message.message_id,
        disable_notification: true,
    }).catch(e => console.error("发送用户回执失败:", e.message)); // 忽略发送失败

    // --- 更新用户的首次消息发送状态 ---
    if (!user.first_message_sent) {
        await dbUserUpdate(userId, { first_message_sent: true }, env);
    }

    // 存储文本消息的原始内容到 messages 表 (用于处理已编辑消息)
    if (message.text) {
        const messageData = {
            text: message.text,
            date: message.date
        };
        await dbMessageDataPut(userId, message.message_id.toString(), messageData, env);
    }
    
    // --- 消息备份转发逻辑 (合并为一条消息) ---
    const backupGroupId = await getConfig('backup_group_id', env, "");
    if (backupGroupId) {
        // 提取用户资料，用于生成备份消息的标题
        const userInfo = getUserInfo(message.from, user.date); 

        // 生成包含发送者信息的标题 (HTML 格式)
        // 注意：在纯文本或媒体配文前添加两行空行分隔
        const fromUserHeader = `
<b>--- 备份消息 ---</b>
👤 <b>来自用户:</b> <a href="tg://user?id=${userInfo.userId}">${userInfo.name || '无昵称'}</a>
• ID: <code>${userInfo.userId}</code>
• 用户名: ${userInfo.username}
------------------
`.trim() + '\n\n'; 
        
        const backupParams = {
            chat_id: backupGroupId,
            disable_notification: true, // 禁用通知
            parse_mode: "HTML",
        };

        try {
            // 1. 尝试处理纯文本消息 (直接合并发送)
            if (message.text) {
                const combinedText = fromUserHeader + message.text;
                await telegramApi(env.BOT_TOKEN, "sendMessage", {
                    ...backupParams,
                    text: combinedText,
                });
                return; // 纯文本已处理，退出
            }

            // 2. 尝试处理带配文的媒体消息 (合并标题到配文，使用 send... 方法)
            let apiMethod = null; // 默认使用 copyMessage 或 fallback
            let payload = { ...backupParams };
            let fileId = null;
            let originalCaption = message.caption || "";
            // 将头部和原有配文合并
            let newCaption = fromUserHeader + originalCaption;

            // 识别媒体类型并准备发送参数
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
            
            // 3. 媒体消息的发送 (使用 send... 方法来合并文本到 caption)
            if (apiMethod && fileId) {
                await telegramApi(env.BOT_TOKEN, apiMethod, payload);
                return; // 媒体消息已处理，退出
            }

            // 4. 复杂内容 (贴纸、投票、转发消息等) - 无法合并
            if (message.sticker || message.poll || message.game || message.forward_from_chat || message.forward_from || message.contact || message.location || message.venue || message.invoice) {
                
                // 无法合并到一条消息，退回到发送两条消息的方案 (头部 + 原始消息内容) 
                
                // 发送头部
                await telegramApi(env.BOT_TOKEN, "sendMessage", {
                    ...backupParams,
                    text: fromUserHeader.trim(), // 只发送标题
                    parse_mode: "HTML",
                });

                // 复制原始消息
                await telegramApi(env.BOT_TOKEN, "copyMessage", {
                    chat_id: backupGroupId,
                    from_chat_id: userId,
                    message_id: message.message_id,
                });
                return; // 复杂内容已处理，退出
            }

        } catch (e) {
            console.error("消息备份转发失败:", e?.message || e);
            // 备份功能失败不应该影响主要转发流程，仅记录错误。
        }
    }
}

/**
* 处理用户在私聊中修改消息的逻辑。
*/
async function handleRelayEditedMessage(editedMessage, env) {
    const { from: user } = editedMessage;
    const userId = user.id.toString();
    
    // 获取用户数据
    const userData = await dbUserGetOrCreate(userId, env);
    const topicId = userData.topic_id;

    if (!topicId) {
        return; 
    }

    // 从 D1 的 messages 表获取原始消息数据
    const storedData = await dbMessageDataGet(userId, editedMessage.message_id.toString(), env);
    let originalText = "[原始内容无法获取/非文本内容]";
    let originalDate = "[发送时间无法获取]";
    
    if (storedData) {
        originalText = storedData.text || originalText;
        originalDate = new Date(storedData.date * 1000).toLocaleString('zh-CN');

        // 更新 D1，将新内容存储为该消息的最新“原始”内容
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

/**
* 处理置顶资料卡消息的操作。
*/
async function handlePinCard(callbackQuery, message, env) {
    const topicId = message.message_thread_id; 
    const adminGroupId = message.chat.id;
    const messageIdToPin = message.message_id; 

    try {
        // 调用 pinChatMessage API
        await telegramApi(env.BOT_TOKEN, "pinChatMessage", {
            chat_id: adminGroupId,
            message_id: messageIdToPin,
            message_thread_id: topicId, // 必须指定话题ID才能在话题内置顶
            disable_notification: true, // 不发出置顶通知
        });

        // 成功反馈
        await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", {
            callback_query_id: callbackQuery.id,
            text: `📌 资料卡已在话题中置顶。`,
            show_alert: false 
        });

    } catch (e) {
         console.error("处理置顶操作失败:", e.message);
         // 失败反馈
         await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", {
            callback_query_id: callbackQuery.id,
            text: `❌ 置顶失败。请确保机器人或有群组的置顶权限。错误信息: ${e.message}`,
            show_alert: true
        });
    }
}


/**
* 处理内联按钮的回调查询。
*/
async function handleCallbackQuery(callbackQuery, env) {
    const { data, message, from: user } = callbackQuery;
    const chatId = message.chat.id.toString();
    const isPrimary = isPrimaryAdmin(user.id, env); // 只有主管理员才能修改配置

    // 检查是否是管理员配置回调
    if (data.startsWith('config:')) {
        if (!isPrimary) {
            await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: callbackQuery.id, text: "您不是主管理员，没有权限执行此操作。", show_alert: true });
            return;
        }
        
        const parts = data.split(':'); 
        const actionType = parts[1]; // menu, edit, toggle, add, list, delete
        const keyOrAction = parts[2]; // base, autoreply, keyword, filter, welcome_msg, enable_image_forwarding...
        const value = parts[3]; // true/false for toggle, ID for delete

        await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: callbackQuery.id, text: "处理中...", show_alert: false });

        // --- 菜单导航处理 ---
        if (actionType === 'menu') {
            // 在导航到子菜单时，我们尝试编辑原消息
            if (keyOrAction === 'base') {
                await handleAdminBaseConfigMenu(chatId, message.message_id, env);
            } else if (keyOrAction === 'autoreply') {
                await handleAdminAutoReplyMenu(chatId, message.message_id, env);
            } else if (keyOrAction === 'keyword') {
                await handleAdminKeywordBlockMenu(chatId, message.message_id, env);
            } else if (keyOrAction === 'filter') {
                await handleAdminTypeBlockMenu(chatId, message.message_id, env);
            // 备份群组菜单导航
            } else if (keyOrAction === 'backup') {
                await handleAdminBackupConfigMenu(chatId, message.message_id, env);
            // 协管员授权菜单导航
            } else if (keyOrAction === 'authorized') {
                await handleAdminAuthorizedConfigMenu(chatId, message.message_id, env);
            } else { // config:menu (主菜单)
                // 刷新主菜单，尝试编辑原消息
                await handleAdminConfigStart(chatId, env);
            }
        // --- 切换开关处理 (用于内容过滤) ---
        } else if (actionType === 'toggle' && keyOrAction && value) {
            await dbConfigPut(keyOrAction, value, env);
            await handleAdminTypeBlockMenu(chatId, message.message_id, env); // 刷新过滤菜单
        // --- 进入编辑模式处理 (用于文本输入: 基础配置/阈值/备份群组 ID/协管员列表) ---
        } else if (actionType === 'edit' && keyOrAction) {
            
            // 清除备份群组 ID 的特殊处理
            if (keyOrAction === 'backup_group_id_clear') {
                await dbConfigPut('backup_group_id', '', env); // 设置为空字符串
                await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: callbackQuery.id, text: `✅ 备份群组 ID 已清除，备份功能已禁用。`, show_alert: false });
                await handleAdminBackupConfigMenu(chatId, message.message_id, env); // 刷新菜单
                return;
            }
            
            // 清除协管员授权列表的特殊处理
             if (keyOrAction === 'authorized_admins_clear') {
                await dbConfigPut('authorized_admins', '[]', env); // 设置为空 JSON 数组
                await telegramApi(env.BOT_TOKEN, "answerCallbackQuery", { callback_query_id: callbackQuery.id, text: `✅ 协管员授权列表已清空。`, show_alert: false });
                await handleAdminAuthorizedConfigMenu(chatId, message.message_id, env); // 刷新菜单
                return;
            }
            
            // 设置管理员状态到 D1
            await dbAdminStatePut(chatId, JSON.stringify({ action: 'awaiting_input', key: keyOrAction }), env);
            
            let prompt = "";
            switch (keyOrAction) {
                case 'welcome_msg': prompt = "请发送**新的欢迎消息**："; break;
                case 'verif_q': prompt = "请发送**新的人机验证问题**："; break;
                case 'verif_a': prompt = "请发送**新的验证答案**："; break;
                case 'block_threshold': prompt = "请发送**屏蔽次数阈值** (纯数字)："; break;
                // 备份群组 ID 输入
                case 'backup_group_id': prompt = "请发送**新的备份群组 ID 或用户名**："; break; 
                // 协管员授权列表输入
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
        // --- 进入新增模式处理 (用于添加规则/关键词) ---
        } else if (actionType === 'add' && keyOrAction) {
            
            // 设置管理员状态到 D1 (使用新的 key 标记添加操作)
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
        // --- 列表模式处理 (显示列表) ---
        } else if (actionType === 'list' && keyOrAction) {
            await handleAdminRuleList(chatId, message.message_id, env, keyOrAction);
        // --- 删除模式处理 (删除单条记录) ---
        } else if (actionType === 'delete' && keyOrAction && value) {
            // value 是要删除的 ID 或关键词字符串
            await handleAdminRuleDelete(chatId, message.message_id, env, keyOrAction, value);
        }
        return; 
    }

    // 非配置相关的操作（屏蔽/置顶）
    if (message.chat.id.toString() !== env.ADMIN_GROUP_ID) {
        return; 
    }

    const [action, userId] = data.split(':');

    // 处理置顶操作
    if (action === 'pin_card') {
        await handlePinCard(callbackQuery, message, env);
        return;
    }

    // 处理屏蔽/解禁操作
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

/**
* 屏蔽用户。
*/
async function handleBlockUser(userId, message, env) {
    try {
        // 设置 D1 中 is_blocked 为 true
        await dbUserUpdate(userId, { is_blocked: true }, env);
        
        // 获取用户信息
        const userData = await dbUserGetOrCreate(userId, env);
        const userName = userData.user_info ? userData.user_info.name : `User ${userId}`;
        
        // 1. 更新按钮状态
        const newMarkup = getInfoCardButtons(userId, true);
        await telegramApi(env.BOT_TOKEN, "editMessageReplyMarkup", {
            chat_id: message.chat.id,
            message_id: message.message_id,
            reply_markup: newMarkup,
        });
        
        // 2. 发送确认消息
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

/**
* 解除屏蔽用户。
*/
async function handleUnblockUser(userId, message, env) {
    try {
        // 删除屏蔽状态 (is_blocked = false) 并清除屏蔽计数 (block_count = 0)
        await dbUserUpdate(userId, { is_blocked: false, block_count: 0 }, env);
        
        // 获取用户信息
        const userData = await dbUserGetOrCreate(userId, env);
        const userName = userData.user_info ? userData.user_info.name : `User ${userId}`;
        
        // 1. 更新按钮状态
        const newMarkup = getInfoCardButtons(userId, false);
        await telegramApi(env.BOT_TOKEN, "editMessageReplyMarkup", {
            chat_id: message.chat.id,
            message_id: message.message_id,
            reply_markup: newMarkup,
        });

        // 2. 发送确认消息
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


/**
 * 将管理员在话题中的回复转发回用户。
 */
async function handleAdminReply(message, env) {
    // 检查是否是话题内的消息
    if (!message.is_topic_message || !message.message_thread_id) return;

    // 检查是否来自管理员群组
    const adminGroupIdStr = env.ADMIN_GROUP_ID.toString();
    if (message.chat.id.toString() !== adminGroupIdStr) return;

    // 忽略机器人自己的消息
    if (message.from && message.from.is_bot) return;

    // 检查消息发送者是否是授权协管员或主管理员
    const senderId = message.from.id.toString();
    const isAuthorizedAdmin = await isAdminUser(senderId, env);
    
    // 如果不是任何一种管理员，则不允许回复中继
    if (!isAuthorizedAdmin) {
        // 为了避免群内干扰，不发送失败提示，直接静默退出。
        return; 
    }

    const topicId = message.message_thread_id.toString();
    // 从 D1 根据 topic_id 查找 user_id
    const userId = await dbTopicUserGet(topicId, env);
    if (!userId) return;

    try {
        // 尝试直接 copyMessage
        await telegramApi(env.BOT_TOKEN, "copyMessage", {
            chat_id: userId,
            from_chat_id: message.chat.id,
            message_id: message.message_id,
        });

    } catch (e) {
        // 如果 copyMessage 失败 (例如：文件太大, 特殊内容)，则尝试 fallback 逐个发送
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
            return; // Fallback 也失败，直接退出，不发送确认消息
        }
    }
    
    // --- 向管理员发送带可点击用户名的回执 (增加开关控制) ---
    // [NEW] 检查配置是否允许发送回执
    const enableAdminReceipt = (await getConfig('enable_admin_receipt', env, 'true')).toLowerCase() === 'true';

    if (enableAdminReceipt) {
        // 首先获取完整的用户信息
        const userData = await dbUserGetOrCreate(userId, env);
        let confirmationDetail;

        // 判断用户是否有用户名，构建不同的回执详情
        if (userData.user_info && userData.user_info.username && userData.user_info.username !== '无') {
            const safeUsername = escapeHtml(userData.user_info.username);
            confirmationDetail = `用户名: <a href="tg://user?id=${userId}">${safeUsername}</a>`;
        } else {
            // 如果没有用户名，回退到显示ID
            confirmationDetail = `ID: <code>${userId}</code>`;
        }

        await telegramApi(env.BOT_TOKEN, "sendMessage", {
            chat_id: message.chat.id,
            message_thread_id: message.message_thread_id,
            text: `✅ 回复已发送给用户 (${confirmationDetail})`,
            parse_mode: "HTML",
            reply_to_message_id: message.message_id,
            disable_notification: true,
        }).catch(e => console.error("发送管理员回执失败:", e.message)); // 忽略发送失败
    }
}
