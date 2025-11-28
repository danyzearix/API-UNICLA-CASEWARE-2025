import express from "express";
import dotenv from "dotenv";
dotenv.config();

/**
 * ================== ENV ==================
 * Requiere:
 *  - KOMMO_BASE_URL  (p.ej: https://crmuniclaretiana.kommo.com)
 *  - KOMMO_TOKEN     (p.ej: Bearer eyJ0eXAi...)
 *  - PORT            (opcional, default 3000)
 *  - LOG_LEVEL       (error|warn|info|debug; default info)
 */
const BASE  = process.env.KOMMO_BASE_URL;
const TOKEN = process.env.KOMMO_TOKEN;
const PORT  = process.env.PORT || 3000;
const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase();

if (!BASE || !TOKEN) {
  console.error("Faltan KOMMO_BASE_URL o KOMMO_TOKEN en variables de entorno");
  process.exit(1);
}

// ============ Logger + correlación ============
const LEVELS = { error:0, warn:1, info:2, debug:3 };
const canLog = (lvl)=> (LEVELS[lvl] <= (LEVELS[LOG_LEVEL] ?? 2));
const slog = (lvl, reqId, ...args) => { if (canLog(lvl)) console.log(new Date().toISOString(), `[${lvl.toUpperCase()}]`, reqId?`(req:${reqId})`:"", ...args); };
const genReqId = ()=> Math.random().toString(36).slice(2,8)+"-"+Date.now().toString(36);

// ================== CONFIG (IDs reales) ==================
const PIPELINE_ID = 11703160;
const STATUS = {
  LEADS_ENTRANTES: 89976996,
  REASIGNACION:    90357456,
  INTERESADO:      89977000,
  PREINSCRITO:     89977004,
  INSCRITO:        89977008,
  ADMITIDO:        89977052,
  MATRICULADO:     89977056,
};
const STAGE_ORDER = [
  "LEADS_ENTRANTES",
  "REASIGNACION",
  "INTERESADO",
  "PREINSCRITO",
  "INSCRITO",
  "ADMITIDO",
  "MATRICULADO",
];

// --- Lead custom fields ---
const LEAD_CF_NUMERO_DOCUMENTO = 2106651; // numeric
const LEAD_CF_PROGRAMA_SINU    = 2102531; // select

// --- Contact custom fields (multitext) ---
const CONTACT_CF_PHONE_ID = 740466; // PHONE
const CONTACT_CF_EMAIL_ID = 740468; // EMAIL

// ================== PROGRAMA SINU mapping ==================
const PROGRAMA_SINU_RAW = [
  { id: 1521401, value: "TSU03  TRABAJO SOCIAL" },
  { id: 1521403, value: "EEB01  ESPECIALIZACIÓN EN ESTUDIOS BÍBLICOS" },
  { id: 1521607, value: "EGF01  ESPECIALIZACIÓN EN GERENCIA FINANCIERA" },
  { id: 1521609, value: "EGPP1  ESPECIALIZACIÓN EN GESTIÓN DE PROCESOS PSICOSOCIALES" },
  { id: 1521611, value: "MGPP1  MAESTRÍA EN GESTIÓN DE PROCESOS PSICOSOCIALES" },
  { id: 1521613, value: "PDE01  DERECHO" },
  { id: 1521615, value: "PII01  INGENIERÍA INDUSTRIAL" },
  { id: 1521617, value: "PIS01  INGENIERÍA DE SISTEMAS" },
  { id: 1521619, value: "PPS01  PSICOLOGÍA" },
  { id: 1521621, value: "TSB04  TRABAJO SOCIAL" },
  { id: 1521623, value: "TSB08  TRABAJO SOCIAL" },
  { id: 1521625, value: "TSC05  TRABAJO SOCIAL" },
  { id: 1521627, value: "TSM06  TRABAJO SOCIAL" },
  { id: 1521629, value: "TSN09  TRABAJO SOCIAL" },
  { id: 1521631, value: "TSP07  TRABAJO SOCIAL" },
  { id: 1521633, value: "TSQ01  TRABAJO SOCIAL" },
  { id: 1521873, value: "TRABAJO SOCIAL" },
  { id: 1521925, value: "TSS02  TRABAJO SOCIAL" },
  { id: 1526889, value: "TRABAJO SOCIAL - ITSMINA" },
  { id: 1526891, value: "INGENIERIA DE SISTEMAS - ITSMINA" },
  { id: 1526893, value: "INGENIERIA INDUSTRIAL - ITSMINA" },
  { id: 1526895, value: "EDUCACIÓN CONTINUADA - ITSMINA" },
  { id: 1526897, value: "DERECHO - ITSMINA" },
  { id: 1526899, value: "PSICOLOGIA - ITSMINA" }
];
const _norm = (s)=> s?.toString().normalize("NFD").replace(/\p{Diacritic}/gu,"").replace(/\s+/g," ").trim().toUpperCase();
const PROGRAMA_SINU_BY_LABEL = Object.fromEntries(PROGRAMA_SINU_RAW.map(x => [x.value, x.id]));
const PROGRAMA_SINU_BY_LABEL_NORM = (()=>{ const m={}; for (const x of PROGRAMA_SINU_RAW) m[_norm(x.value)] = x.id; return m; })();
const getProgramaEnumId = (txt)=> {
  if (!txt) return null;
  if (PROGRAMA_SINU_BY_LABEL[txt]) return PROGRAMA_SINU_BY_LABEL[txt];
  return PROGRAMA_SINU_BY_LABEL_NORM[_norm(txt)] ?? null;
};

// ================== Utils ==================
const authHeaders = { Authorization: TOKEN, Accept: "application/hal+json", "Content-Type": "application/json" };
const normEmail = (x)=> (x||"").trim().toLowerCase();
const normPhone = (x)=> (x||"").toString().replace(/[^\d]/g,"");
const normText  = (x)=> (x||"").replace(/\s+/g," ").trim();
const rank      = (e)=> STAGE_ORDER.indexOf(normalizeEstado(e));

function normalizeEstado(raw=""){
  const t = raw.normalize("NFD").replace(/\p{Diacritic}/gu,"").toUpperCase();
  if (t.includes("LEADS") || t.includes("ENTR")) return "LEADS_ENTRANTES";
  if (t.includes("REASIG")) return "REASIGNACION";
  if (t.includes("INTERES")) return "INTERESADO";
  if (t.includes("MATR")) return "MATRICULADO";
  if (t.includes("ADMI")) return "ADMITIDO";
  if (t.includes("INSCR")) return "INSCRITO";
  return "PREINSCRITO";
}
function estadoToStatusId(e){ return STATUS[normalizeEstado(e)] ?? STATUS.PREINSCRITO; }

// CF helpers
const getCF = (arr, id)=> (arr||[]).find(f => f.field_id === id);
const getLeadProgramaEnumId = (lead)=> getCF(lead.custom_fields_values, LEAD_CF_PROGRAMA_SINU)?.values?.[0]?.enum_id ?? null;
const getLeadDocumento      = (lead)=> {
  const f = getCF(lead.custom_fields_values, LEAD_CF_NUMERO_DOCUMENTO);
  return f?.values?.[0]?.value ? String(f.values[0].value) : null;
};
function extractContactChannels(contact){
  let emails = [], phones = [];
  for (const f of (contact.custom_fields_values||[])) {
    if (f.field_id === CONTACT_CF_EMAIL_ID)  for (const v of (f.values||[])) if (v.value) emails.push(normEmail(v.value));
    if (f.field_id === CONTACT_CF_PHONE_ID)  for (const v of (f.values||[])) if (v.value) phones.push(normPhone(v.value));
  }
  return { emails, phones };
}

// ================== Fetch snapshot del pipeline ==================
async function kfetch(path, opts={}, reqId){
  const url = `${BASE}${path}`;
  const method = (opts.method||"GET").toUpperCase();
  if (opts.body && canLog("debug")) try { const parsed = JSON.parse(opts.body); slog("debug", reqId, `HTTP → ${method} ${path}`, Array.isArray(parsed)?`[${parsed.length}]`:parsed); } catch { slog("debug", reqId, `HTTP → ${method} ${path}`, "(raw body)"); }
  else slog("debug", reqId, `HTTP → ${method} ${path}`);
  const t0 = Date.now();
  const res = await fetch(url, { ...opts, headers: { ...authHeaders, ...(opts.headers||{}) } });
  const dt = Date.now()-t0;
  const text = await res.text();
  if (!res.ok) {
    slog("warn", reqId, `HTTP ← ${res.status} ${res.statusText} ${path} (${dt}ms)`, text.slice(0,400));
    throw new Error(`${res.status} ${res.statusText} :: ${path} :: ${text.slice(0,400)}`);
  }
  slog("debug", reqId, `HTTP ← ${res.status} ${path} (${dt}ms)`);
  return text ? JSON.parse(text) : null;
}

async function fetchLeadsByStatus(statusId, reqId){
  const all = [];
  let page = 1;
  const limit = 250;
  slog("info", reqId, `Leads status=${statusId} pageando...`);
  while (true) {
    const q = `/api/v4/leads?with=contacts,custom_fields_values&filter[statuses][0][pipeline_id]=${PIPELINE_ID}&filter[statuses][0][status_id]=${statusId}&page=${page}&limit=${limit}`;
    const data  = await kfetch(q, {}, reqId);
    const chunk = data?._embedded?.leads || [];
    all.push(...chunk);
    if (!data?._links?.next?.href || chunk.length < limit) break;
    page++;
  }
  slog("info", reqId, `  total=${all.length} en status=${statusId}`);
  return all;
}

async function fetchAllLeadsForPipeline(reqId){
  const statuses = [
    STATUS.LEADS_ENTRANTES,
    STATUS.REASIGNACION,
    STATUS.INTERESADO,
    STATUS.PREINSCRITO,
    STATUS.INSCRITO,
    STATUS.ADMITIDO,
    STATUS.MATRICULADO
  ];
  const out = [];
  slog("info", reqId, `Cargando snapshot pipeline ${PIPELINE_ID} (${statuses.length} etapas)`);
  for (const st of statuses) {
    const part = await fetchLeadsByStatus(st, reqId);
    out.push(...part);
  }
  slog("info", reqId, `Snapshot listo, leads=${out.length}`);
  return out;
}

// ================== Índices en memoria ==================
function buildIndexes(leads){
  const byDoc = new Map();
  const byEmail = new Map();
  const byPhone = new Map();

  for (const ld of leads) {
    const doc = getLeadDocumento(ld);
    if (doc) {
      const k = String(doc);
      if (!byDoc.has(k)) byDoc.set(k, []);
      byDoc.get(k).push(ld);
    }
    const c = ld._embedded?.contacts?.[0];
    if (c) {
      const ch = extractContactChannels(c);
      for (const e of (ch.emails||[])) {
        if (!byEmail.has(e)) byEmail.set(e, []);
        byEmail.get(e).push(ld);
      }
      for (const p of (ch.phones||[])) {
        if (!byPhone.has(p)) byPhone.set(p, []);
        byPhone.get(p).push(ld);
      }
    }
  }
  return { byDoc, byEmail, byPhone };
}

function relatedLeadsFromIndex({ documento, email, phone }, idx){
  const set = new Set();
  if (documento && idx.byDoc.has(String(documento))) idx.byDoc.get(String(documento)).forEach(ld => set.add(ld));
  const ne = normEmail(email); if (ne && idx.byEmail.has(ne)) idx.byEmail.get(ne).forEach(ld => set.add(ld));
  const np = normPhone(phone); if (np && idx.byPhone.has(np)) idx.byPhone.get(np).forEach(ld => set.add(ld));
  return Array.from(set);
}

function currentEstadoFromStatusId(sid){
  if (sid === STATUS.LEADS_ENTRANTES) return "LEADS_ENTRANTES";
  if (sid === STATUS.REASIGNACION)    return "REASIGNACION";
  if (sid === STATUS.INTERESADO)      return "INTERESADO";
  if (sid === STATUS.PREINSCRITO)     return "PREINSCRITO";
  if (sid === STATUS.INSCRITO)        return "INSCRITO";
  if (sid === STATUS.ADMITIDO)        return "ADMITIDO";
  if (sid === STATUS.MATRICULADO)     return "MATRICULADO";
  return "PREINSCRITO";
}

// ================== App ==================
const app = express();
app.use(express.json({ limit: "10mb" }));

/**
 * POST /plan-batch
 * Body: array de objetos SGA (>= 2)
 * Salida: { summary, to_advance[], to_create[] }
 *   - NO ejecuta escrituras a Kommo
 */
app.post("/plan-batch", async (req, res) => {
  const reqId = genReqId();
  try {
    if (!Array.isArray(req.body) || req.body.length < 2) {
      return res.status(400).json({ ok:false, error:"Envíe un array con 2 o más objetos" });
    }
    const items = req.body;
    slog("info", reqId, `>> /plan-batch items=${items.length}`);

    // 1) Cargar snapshot de leads del pipeline
    const snapshot = await fetchAllLeadsForPipeline(reqId);
    const index = buildIndexes(snapshot);

    const to_advance = [];
    const to_create  = [];

    // 2) Evaluar cada ítem (solo lectura)
    items.forEach((raw, i) => {
      const documento = (raw.IDENTIFICACION||"").toString().trim();
      const nombre    = normText(raw.NOMBRE||"");
      const email     = normEmail(raw.EMAIL||"");
      const phone     = normPhone(raw.CELULAR||"");
      const estadoT   = normalizeEstado(raw.ESTADO||"PREINSCRITO");
      const progEnum  = getProgramaEnumId(raw.PROGRAMA);

      slog("debug", reqId, `Item#${i} doc=${documento} email=${email} phone=${phone} estado=${estadoT} progEnum=${progEnum}`);

      // Encontrar leads relacionados (por doc/email/phone)
      const related = relatedLeadsFromIndex({ documento, email, phone }, index);

      // Clasificar por programa (elige el de mayor etapa si hay varios)
      const sameProgram = related
        .filter(ld => getLeadProgramaEnumId(ld) === progEnum)
        .sort((a,b) => rank(currentEstadoFromStatusId(b.status_id)) - rank(currentEstadoFromStatusId(a.status_id)));
      const noProgram   = related.filter(ld => !getLeadProgramaEnumId(ld));
      const diffProgram = related.filter(ld => {
        const pid = getLeadProgramaEnumId(ld);
        return pid && progEnum && pid !== progEnum;
      });

      // 2.1 Casos que requieren avance (mismo programa y target > current)
      if (sameProgram.length > 0) {
        const best = sameProgram[0];
        const from = currentEstadoFromStatusId(best.status_id);
        const shouldAdvance = rank(estadoT) > rank(from);
        if (shouldAdvance) {
          to_advance.push({
            idx: i,
            lead_id: best.id,
            contact_id: best._embedded?.contacts?.[0]?.id ?? null,
            programa_enum_id: progEnum,
            from_stage: from,
            to_stage: estadoT,
            from_status_id: best.status_id,
            to_status_id: estadoToStatusId(estadoT),
            documento: documento || null,
            email: email || null,
            phone: phone || null,
            reason: "stage_ahead"
          });
          slog("info", reqId, `PLAN avance -> lead_id=${best.id} ${from} -> ${estadoT}`);
          return; // ya decidido
        } else {
          // No entra a "advance"; tampoco se crea, así que no hacemos nada
          slog("debug", reqId, `Sin cambio (mismo programa y etapa >= target) lead_id=${best.id}`);
          return;
        }
      }

      // 2.2 Casos de creación: (a) programa distinto -> lead nuevo para ese programa
      if (diffProgram.length > 0) {
        to_create.push({
          idx: i,
          reason: "different_program",
          suggested_contact: {
            name: nombre || raw.PROGRAMA || "Lead SGA",
            email: email || undefined,
            phone: phone || undefined
          },
          suggested_lead: {
            name: nombre || raw.PROGRAMA || "Lead SGA",
            pipeline_id: PIPELINE_ID,
            status_id: estadoToStatusId(estadoT),
            custom_fields_values: [
              ...(progEnum ? [{ field_id: LEAD_CF_PROGRAMA_SINU, values: [{ enum_id: progEnum }] }] : []),
              ...(documento ? [{ field_id: LEAD_CF_NUMERO_DOCUMENTO, values: [{ value: String(documento) }] }] : [])
            ]
          }
        });
        slog("info", reqId, `PLAN crear -> (programa distinto) idx=${i}`);
        return;
      }

      // 2.3 Casos de creación: (b) sin leads relacionados
      if (related.length === 0) {
        to_create.push({
          idx: i,
          reason: "no_match",
          suggested_contact: {
            name: nombre || raw.PROGRAMA || "Lead SGA",
            email: email || undefined,
            phone: phone || undefined
          },
          suggested_lead: {
            name: nombre || raw.PROGRAMA || "Lead SGA",
            pipeline_id: PIPELINE_ID,
            status_id: estadoToStatusId(estadoT),
            custom_fields_values: [
              ...(progEnum ? [{ field_id: LEAD_CF_PROGRAMA_SINU, values: [{ enum_id: progEnum }] }] : []),
              ...(documento ? [{ field_id: LEAD_CF_NUMERO_DOCUMENTO, values: [{ value: String(documento) }] }] : [])
            ]
          }
        });
        slog("info", reqId, `PLAN crear -> (sin coincidencias) idx=${i}`);
        return;
      }

      // 2.4 Caso ignorado: hay lead(s) relacionados pero sin programa (sería completar, no crear)
      if (noProgram.length > 0) {
        slog("debug", reqId, `Relacionado sin programa (no se incluye en listas) idx=${i}`);
        return;
      }

      // Default: nada que hacer
      slog("debug", reqId, `Sin acción idx=${i}`);
    });

    const summary = {
      total_in: items.length,
      total_snapshot: snapshot.length,
      to_advance: to_advance.length,
      to_create:  to_create.length
    };

    slog("info", reqId, `PLAN resumen -> advance=${to_advance.length} create=${to_create.length}`);
    return res.json({ ok:true, summary, to_advance, to_create });

  } catch (err) {
    const reqId = genReqId();
    slog("error", reqId, "plan-batch ERROR:", err?.message || err);
    return res.status(500).json({ ok:false, error: err.message || String(err) });
  }
});

// healthcheck
app.get("/health", (_req, res) => res.json({ ok:true, mode:"plan-only", pipeline: PIPELINE_ID, base: BASE }));

app.listen(PORT, ()=> console.log(`[BOOT] Kommo Plan API (read-only) :${PORT} base=${BASE} log=${LOG_LEVEL}`));
