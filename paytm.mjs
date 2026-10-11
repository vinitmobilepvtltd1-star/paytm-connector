// Paytm Settlement API ka saara logic yahan hai.
// index.mjs (Claude connector) aur test.mjs (command line test) dono isi ko use karte hain.

import PaytmChecksum from "paytmchecksum";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// Paytm ke do server address. Pehla naya hai, doosra purana. Ek na chale to doosra try hota hai.
const HOSTS = [
  "https://secure.paytmpayments.com",
  "https://securegw.paytm.in",
];
const SETTLEMENT_PATH = "/merchant-settlement-service/settlement/list";

// Checksum banane ke do tareeke. Jo chal jaye wo yaad rakha jata hai.
const SIGN_MODES = ["json", "params"];

let working = null; // { host, mode } jo pehli baar safal hua

// MID aur key: Render par PAYTM_MIDS environment variable se, warna mids.json file se.
export function loadMids() {
  let list;
  if (process.env.PAYTM_MIDS) {
    const v = JSON.parse(process.env.PAYTM_MIDS);
    list = Array.isArray(v) ? v : v.mids;
  } else {
    const file = path.join(__dirname, "mids.json");
    list = JSON.parse(fs.readFileSync(file, "utf8")).mids;
  }
  return (list || []).filter((m) => m.mid && m.key);
}

export function findMids(mids, query) {
  if (!query) return mids;
  const q = String(query).trim().toLowerCase();
  return mids.filter(
    (m) =>
      m.mid.toLowerCase() === q ||
      (m.shop && m.shop.toLowerCase().includes(q))
  );
}

export function yesterday() {
  // India time ke hisaab se kal ki date
  const now = new Date(Date.now() + 5.5 * 3600 * 1000);
  now.setUTCDate(now.getUTCDate() - 1);
  return now.toISOString().slice(0, 10);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function sign(body, key, mode) {
  if (mode === "json") {
    return PaytmChecksum.generateSignature(JSON.stringify(body), key);
  }
  const strBody = Object.fromEntries(Object.entries(body).map(([k, v]) => [k, String(v)]));
  return PaytmChecksum.generateSignature(strBody, key);
}

async function postOnce(host, mode, m, date, pageNum) {
  const body = {
    MID: m.mid,
    utrProcessedStartTime: date,
    pageNum,
    pageSize: 20,
  };
  const checksumHash = await sign(body, m.key, mode);
  const res = await fetch(host + SETTLEMENT_PATH, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...body, checksumHash }),
  });
  const text = await res.text();
  let json;
  try {
    json = JSON.parse(text);
  } catch {
    json = { status: "FAILURE", errorMessage: `HTTP ${res.status}: ${text.slice(0, 300)}` };
  }
  return json;
}

function isChecksumFail(j) {
  return (
    j &&
    (j.resultCode === "00000022" ||
      String(j.errorMessage || "").toUpperCase().includes("CHECKSUM"))
  );
}

// Ek page laana. Pehli baar sahi server + checksum tareeka khud dhoondhta hai.
export async function fetchPage(m, date, pageNum) {
  const combos = working
    ? [working]
    : HOSTS.flatMap((host) => SIGN_MODES.map((mode) => ({ host, mode })));

  let last = null;
  for (const c of combos) {
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        const j = await postOnce(c.host, c.mode, m, date, pageNum);
        if (j.resultCode === "00000074") {
          // Bahut jaldi request - thoda ruk kar dobara
          await sleep(1500);
          continue;
        }
        last = { ...j, _host: c.host, _mode: c.mode };
        if (isChecksumFail(j)) break; // agla tareeka try karo
        if (j.status === "SUCCESS" || j.status === "RECORD_NOT_FOUND") {
          working = c;
        }
        return last;
      } catch (e) {
        last = { status: "FAILURE", errorMessage: String(e.message || e), _host: c.host, _mode: c.mode };
        break; // network error - agla server try karo
      }
    }
  }
  return last;
}

// Paytm field naam CAPITAL me bhejta hai (TXNAMOUNT, UTR, POSID...). Unhe ek jaise naam me badalna.
function normRow(t) {
  const g = (...keys) => {
    for (const k of keys) if (t[k] !== undefined && t[k] !== null) return t[k];
    return undefined;
  };
  return {
    txnId: g("TXNID", "txnId"),
    txnType: g("TXNTYPE", "txnType"),
    txnDate: g("TXNDATE", "txnDate"),
    txnAmount: g("TXNAMOUNT", "txnAmount"),
    paymentMode: g("PAYMENTMODE", "paymentMode"),
    bankName: g("BANKNAME", "bankName"),
    mid: g("MID", "mid"),
    merchantName: g("MERCHANTNAME", "merchantName"),
    orderId: g("ORDERID", "orderId"),
    posId: g("POSID", "posId"),
    utr: g("UTR", "utr"),
    utrProcessedTime: g("UTRPROCESSEDTIME", "utrProcessedTime"),
    settleAmount: g("SETTLEDAMOUNT", "settleAmount"),
    payoutDate: g("PAYOUT DATE", "payoutDate"),
    settledDate: g("SETTLED DATE", "settledDate"),
    commission: g("COMMISSION", "commission"),
    gst: g("GST", "gst"),
    acquiringFee: g("ACQUIRINGFEE", "ACQUIRING FEE", "acquiringFee"),
    acquiringTax: g("ACQUIRINGTAX", "ACQUIRING TAX", "acquiringTax"),
  };
}

// Ek MID ke ek din ke saare settled transactions (saare pages)
export async function fetchDay(m, date) {
  const rows = [];
  let page = 1;
  let totalPages = 1;
  let firstResp = null;
  do {
    const j = await fetchPage(m, date, page);
    if (!firstResp) firstResp = j;
    if (!j || j.status !== "SUCCESS") {
      return {
        ok: j && j.status === "RECORD_NOT_FOUND",
        status: j ? j.status : "FAILURE",
        error: j && j.status !== "RECORD_NOT_FOUND" ? (j.errorMessage || j.resultCode || "Unknown error") : null,
        rows,
      };
    }
    const resp = j.settlementListResponse || j;
    const list = resp.settlementTransactionList || resp.settlementDetailList || [];
    rows.push(...list.map(normRow));
    totalPages = Number(resp.paginatorTotalPage || 1);
    page++;
    if (page <= totalPages) await sleep(300);
  } while (page <= totalPages);
  return { ok: true, status: "SUCCESS", error: null, rows };
}

const num = (v) => {
  const n = parseFloat(v);
  return Number.isFinite(n) ? n : 0;
};
const r2 = (n) => Math.round(n * 100) / 100;

export function summarize(rows) {
  const byType = {};
  const byMode = {};
  const byPos = {};
  const utrs = {};
  let fees = 0;
  let settled = 0;
  for (const t of rows) {
    const type = t.txnType || "UNKNOWN";
    const amt = num(t.txnAmount);
    byType[type] = byType[type] || { count: 0, amount: 0 };
    byType[type].count++;
    byType[type].amount += amt;

    if (type === "TRANSACTION") {
      const mode = t.paymentMode || "UNKNOWN";
      byMode[mode] = byMode[mode] || { count: 0, amount: 0 };
      byMode[mode].count++;
      byMode[mode].amount += amt;

      const pos = t.posId || "(POS nahi)";
      byPos[pos] = byPos[pos] || { posId: pos, count: 0, amount: 0 };
      byPos[pos].count++;
      byPos[pos].amount += amt;
    }

    fees += num(t.commission) + num(t.gst) + num(t.acquiringFee) + num(t.acquiringTax);
    settled += num(t.settleAmount);

    if (t.utr) {
      utrs[t.utr] = utrs[t.utr] || { utr: t.utr, utrProcessedTime: t.utrProcessedTime, txnCount: 0, txnAmount: 0, settledAmount: 0 };
      utrs[t.utr].txnCount++;
      utrs[t.utr].txnAmount += amt;
      utrs[t.utr].settledAmount += num(t.settleAmount);
    }
  }
  for (const k in byType) byType[k].amount = r2(byType[k].amount);
  for (const k in byMode) byMode[k].amount = r2(byMode[k].amount);
  for (const k in byPos) byPos[k].amount = r2(byPos[k].amount);

  return {
    totalRows: rows.length,
    saleAmount: byType.TRANSACTION ? byType.TRANSACTION.amount : 0,
    refundAmount: byType.REFUND ? byType.REFUND.amount : 0,
    totalFees: r2(fees),
    settledToBank: r2(settled),
    byTxnType: byType,
    saleByPaymentMode: byMode,
    saleByMachinePOS: Object.values(byPos),
    bankCredits: Object.values(utrs).map((u) => ({
      ...u,
      txnAmount: r2(u.txnAmount),
      settledAmount: r2(u.settledAmount),
    })),
  };
}

export function slimRow(t) {
  return {
    txnDate: t.txnDate,
    txnType: t.txnType,
    amount: t.txnAmount,
    paymentMode: t.paymentMode,
    posId: t.posId,
    orderId: t.orderId,
    txnId: t.txnId,
    fees: r2(num(t.commission) + num(t.gst) + num(t.acquiringFee) + num(t.acquiringTax)),
    utr: t.utr,
    settledAmount: t.settleAmount,
    settledDate: t.settledDate,
  };
}

export function connectionInfo() {
  return working;
}
