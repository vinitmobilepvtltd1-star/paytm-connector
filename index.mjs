// Vinit Mobile - Paytm connector for Claude Desktop
// Claude ko Paytm settlement data dene wala MCP server.

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";
import {
  loadMids,
  findMids,
  yesterday,
  fetchDay,
  fetchPage,
  summarize,
  slimRow,
  connectionInfo,
} from "./paytm.mjs";

const server = new McpServer({ name: "paytm", version: "1.0.0" });

const out = (obj) => ({
  content: [{ type: "text", text: JSON.stringify(obj, null, 2) }],
});

server.tool(
  "paytm_mids",
  "Paytm ke saare MID (merchant ID) aur unki shop ka naam dikhata hai. Kaunsa MID kis shop ka hai ye dekhne ke liye.",
  {},
  async () => {
    const mids = loadMids();
    return out({
      total: mids.length,
      mids: mids.map((m) => ({ mid: m.mid, shop: m.shop || "(shop naam nahi dala)" })),
    });
  }
);

server.tool(
  "paytm_settlement",
  "Paytm machine/QR ke settled transactions aur bank me aaye settlement (UTR) ka data deta hai. " +
    "date = settlement date (YYYY-MM-DD), khali chhodo to kal ki date. " +
    "shop_or_mid khali chhodo to saare MID ka summary aata hai. " +
    "Note: ye sirf settle ho chuke transactions dikhata hai; aaj ke live payment settle hone ke baad aate hain. Max pichle 6 mahine.",
  {
    date: z.string().optional().describe("Settlement date YYYY-MM-DD"),
    shop_or_mid: z.string().optional().describe("MID ya shop ka naam (ya uska hissa)"),
    include_transactions: z
      .boolean()
      .optional()
      .describe("Har transaction ki list bhi chahiye to true. Default: ek MID ho to true, sab ho to false"),
  },
  async ({ date, shop_or_mid, include_transactions }) => {
    const d = date || yesterday();
    const all = loadMids();
    const mids = findMids(all, shop_or_mid);
    if (mids.length === 0) {
      return out({ error: `"${shop_or_mid}" se koi MID/shop nahi mila. paytm_mids se list dekho.` });
    }
    const withTxns = include_transactions ?? mids.length === 1;

    const results = [];
    const grand = { saleAmount: 0, refundAmount: 0, totalFees: 0, settledToBank: 0, totalRows: 0 };
    for (const m of mids) {
      const r = await fetchDay(m, d);
      const s = summarize(r.rows);
      for (const k in grand) grand[k] = Math.round((grand[k] + s[k]) * 100) / 100;
      const item = {
        mid: m.mid,
        shop: m.shop || "",
        status: r.status,
        error: r.error,
        summary: s,
      };
      if (withTxns) item.transactions = r.rows.map(slimRow);
      results.push(item);
    }

    return out({
      settlementDate: d,
      midsChecked: mids.length,
      grandTotal: grand,
      results,
      connection: connectionInfo(),
    });
  }
);

server.tool(
  "paytm_raw",
  "Testing ke liye: ek MID ka Paytm se aaya asli (raw) jawab page 1 dikhata hai. Koi error aaye to iska use karo.",
  {
    shop_or_mid: z.string().describe("MID ya shop ka naam"),
    date: z.string().optional().describe("YYYY-MM-DD, default kal"),
  },
  async ({ shop_or_mid, date }) => {
    const m = findMids(loadMids(), shop_or_mid)[0];
    if (!m) return out({ error: "MID nahi mila" });
    const j = await fetchPage(m, date || yesterday(), 1);
    return out(j);
  }
);

await server.connect(new StdioServerTransport());
