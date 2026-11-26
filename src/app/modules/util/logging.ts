import { UUID } from "crypto";
import { apiDB } from "./resourceManagers/db.js";
import { emailAdminError } from "./email.js";

const FLUSH_THRESHOLD = 1000;
const FLUSH_INTERVAL_MS = 5000;
const COLS_PER_ROW = 23;

const logs: Map<UUID, RequestLogData> = new Map<UUID, RequestLogData>();
let activeFlush: Promise<void> = Promise.resolve();

const flushTimer = setInterval(() => {
  if(logs.size > 0) {
    flushLogs();
  }
}, FLUSH_INTERVAL_MS);
flushTimer.unref();

async function flushBatch(batch: RequestLogData[]) {
  const valuesSql: string[] = [];
  const params: any[] = [];

  for(let i = 0; i < batch.length; i++) {
    const data = batch[i];
    const rowIndex = i * COLS_PER_ROW;
    const rowValues = [];
    for(let j = 1; j <= COLS_PER_ROW; j++) {
      rowValues.push(`$${rowIndex + j}`);
    }
    valuesSql.push(`(${rowValues.join(", ")})`);

    params.push(
      data.requestId,
      data.apiVersion,
      data.userIp,
      data.userAgent,
      data.method,
      data.endpoint,
      data.template,
      data.query,
      data.startTime,
      data.returnTime,
      data.endTime,
      data.returnDurationMs,
      data.durationMs,
      data.email,
      data.token,
      data.tokenUser,
      data.code,
      data.success,
      data.error,
      data.errorHandled,
      data.adminEmailed,
      data.sizeF,
      data.sizeB
    );
  }

  const query = `
    INSERT INTO api_logs (
      request_id,
      api_version,
      user_ip,
      user_agent,
      method,
      endpoint,
      template,
      query,
      start_time,
      return_time,
      end_time,
      return_duration_ms,
      duration_ms,
      email,
      token,
      token_user,
      code,
      success,
      error,
      error_handled,
      admin_emailed,
      size_f,
      size_b
    )
    VALUES ${valuesSql.join(",\n")}
    ON CONFLICT (request_id) DO UPDATE SET
      return_time = EXCLUDED.return_time,
      end_time = EXCLUDED.end_time,
      return_duration_ms = EXCLUDED.return_duration_ms,
      duration_ms = EXCLUDED.duration_ms,
      template = EXCLUDED.template,
      email = EXCLUDED.email,
      token = EXCLUDED.token,
      token_user = EXCLUDED.token_user,
      code = EXCLUDED.code,
      success = EXCLUDED.success,
      error = EXCLUDED.error,
      error_handled = EXCLUDED.error_handled,
      admin_emailed = EXCLUDED.admin_emailed,
      size_f = EXCLUDED.size_f,
      size_b = EXCLUDED.size_b;
  `;

  try {
    await apiDB.queryNoRes(query, params);
  }
  catch(e) {
    const errMessage = `Failed to write API log batch (${batch.length} entries): ${e}`;
    console.error(errMessage);
    await emailAdminError(errMessage).catch((mailErr) => {
      console.error("Failed to send admin error email:", mailErr);
    });
  }
}

export function flushLogs(): Promise<void> {
  activeFlush = activeFlush.then(async () => {
    if(logs.size === 0) {
      return;
    }

    const entries = Array.from(logs.values());
    logs.clear();

    for(let i = 0; i < entries.length; i += FLUSH_THRESHOLD) {
      await flushBatch(entries.slice(i, i + FLUSH_THRESHOLD));
    }
  });

  return activeFlush;
}

export function logReq(data: RequestLogData) {
  logs.set(data.requestId, { ...data });
  if(logs.size >= FLUSH_THRESHOLD) {
    void flushLogs();
  }
}

export type RequestLogData = {
  requestId: UUID,

  apiVersion: string,

  userIp: string | null,
  userAgent: string | null,

  method: string,
  endpoint: string,
  template: string | null,
  query: string | null,

  startTime: Date,
  returnTime: Date | null,
  endTime: Date | null,
  returnDurationMs: number | null,
  durationMs: number | null,

  email: string | null,
  token: string | null,
  tokenUser: string | null,

  code: number | null,
  success: boolean | null,
  error: string | null,
  errorHandled: boolean | null,
  adminEmailed: boolean | null,

  sizeF: number | null,
  sizeB: number | null
};