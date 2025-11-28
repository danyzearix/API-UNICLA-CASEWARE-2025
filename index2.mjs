import express from "express";
import dotenv from 'dotenv';
dotenv.config();

// ================== ENV ==================
const BASE  = process.env.KOMMO_BASE_URL;   // ej: https://crmuniclaretiana.kommo.com
const TOKEN = process.env.KOMMO_TOKEN;      // ej: Bearer eyJ0eXAi...
const PORT  = process.env.PORT || 3000;
const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase();
const BATCH_CONCURRENCY = Number(process.env.BATCH_CONCURRENCY || 5);

// ============ Logger con niveles + correlación ============
const LEVELS = { error:0, warn:1, info:2, debug:3 };
function canLog(level) { return LEVELS[level] <= (LEVELS[LOG_LEVEL] ?? 2); }
function slog(level, reqId, ...args) {
  if (!canLog(level)) return;
  const stamp = new Date().toISOString();
  console.log(stamp, `[${level.toUpperCase()}]`, reqId ? `(req:${reqId})` : "", ...args);
}
function genReqId() { return Math.random().toString(36).slice(2,8) + "-" + Date.now().toString(36); }

if (!BASE || !TOKEN) {
  console.error("Faltan KOMMO_BASE_URL o KOMMO_TOKEN en variables de entorno");
  process.exit(1);
}

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

// ranking para avance
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
const PHONE_ENUM_MOB      = 525936; // MOB
const CONTACT_CF_EMAIL_ID = 740468; // EMAIL
const EMAIL_ENUM_WORK     = 525944; // WORK

// ================== PROGRAMA SINU (normalización robusta) ==================
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

const _norm = (s) => s?.toString()
  .normalize("NFD").replace(/\p{Diacritic}/gu, "")
  .replace(/\s+/g, " ").trim().toUpperCase();

const PROGRAMA_SINU_BY_LABEL = Object.fromEntries(PROGRAMA_SINU_RAW.map(x => [x.value, x.id]));
const PROGRAMA_SINU_BY_LABEL_NORM = (() => {
  const m = {};
  for (const x of PROGRAMA_SINU_RAW) m[_norm(x.value)] = x.id;
  return m;
})();
function getProgramaEnumId(programaTexto) {
  if (!programaTexto) return null;
  if (PROGRAMA_SINU_BY_LABEL[programaTexto]) return PROGRAMA_SINU_BY_LABEL[programaTexto];
  const k = _norm(programaTexto);
  return PROGRAMA_SINU_BY_LABEL_NORM[k] ?? null;
}

// ===== Helpers de batch =====
function isValidItem(it) {
  const hasDoc = !!(it?.IDENTIFICACION && String(it.IDENTIFICACION).trim());
  const hasEmail = !!(it?.EMAIL && String(it.EMAIL).trim());
  const hasPhone = !!(it?.CELULAR && String(it.CELULAR).trim());
  return hasDoc || hasEmail || hasPhone;
}

// ================== HELPERS ==================
const app = express();
app.use(express.json({ limit: "5mb" }));

const authHeaders = {
  Authorization: TOKEN,
  Accept: "application/hal+json",
  "Content-Type": "application/json",
};

async function kfetch(path, opts = {}, reqId) {
  const method = (opts.method || "GET").toUpperCase();
  const url = `${BASE}${path}`;
  if (opts.body && canLog("debug")) {
    try {
      const parsed = JSON.parse(opts.body);
      slog("debug", reqId, `HTTP → ${method} ${path}`, "payload:", Array.isArray(parsed) ? `[${parsed.length}]` : parsed);
    } catch {
      slog("debug", reqId, `HTTP → ${method} ${path}`, "payload(raw):", String(opts.body).slice(0, 500));
    }
  } else {
    slog("debug", reqId, `HTTP → ${method} ${path}`);
  }

  const t0 = Date.now();
  const res = await fetch(url, { ...opts, headers: { ...authHeaders, ...(opts.headers||{}) } });
  const dt = Date.now() - t0;

  if (!res.ok) {
    const text = await res.text().catch(()=> "");
    slog("error", reqId, `HTTP ← ${res.status} ${res.statusText} ${path} (${dt}ms)`, "body:", text?.slice(0, 800));
    throw new Error(`${res.status} ${res.statusText} :: ${path} :: ${text}`);
  }

  if (res.status === 204) {
    slog("debug", reqId, `HTTP ← ${res.status} (no content) ${path} (${dt}ms)`);
    return null;
  }
  const json = await res.json().catch(async() => {
    const text = await res.text().catch(()=> "");
    slog("warn", reqId, `HTTP ← parse warn ${path} (${dt}ms)`, text?.slice(0, 500));
    return null;
  });
  slog("debug", reqId, `HTTP ← 200 ${path} (${dt}ms)`);
  return json;
}

const normEmail = (x)=> (x||"").trim().toLowerCase();
const normPhone = (x)=> (x||"").toString().replace(/[^\d]/g,"");
const normText  = (x)=> (x||"").replace(/\s+/g," ").trim();

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
function estadoToStatusId(e){
  const n = normalizeEstado(e);
  return STATUS[n] ?? STATUS.PREINSCRITO;
}
function rank(e){ return STAGE_ORDER.indexOf(normalizeEstado(e)); }

function getCF(ary, fieldId){
  return (ary||[]).find(f => f.field_id === fieldId);
}
function getLeadProgramaEnumId(lead){
  const f = getCF(lead.custom_fields_values, LEAD_CF_PROGRAMA_SINU);
  return f?.values?.[0]?.enum_id ?? null;
}
function getLeadDocumento(lead){
  const f = getCF(lead.custom_fields_values, LEAD_CF_NUMERO_DOCUMENTO);
  return f?.values?.[0]?.value ? String(f.values[0].value) : null;
}
function extractContactChannels(contact){
  let emails = [], phones = [];
  for (const f of (contact.custom_fields_values||[])) {
    if (f.field_id === CONTACT_CF_EMAIL_ID) {
      for (const v of (f.values||[])) if (v.value) emails.push(normEmail(v.value));
    }
    if (f.field_id === CONTACT_CF_PHONE_ID) {
      for (const v of (f.values||[])) if (v.value) phones.push(normPhone(v.value));
    }
  }
  return { emails, phones };
}

// ================== CARGA TOTAL DE LEADS DEL PIPELINE (snapshot + índices) ==================
async function fetchLeadsByStatus(statusId, reqId){
  const all = [];
  let page = 1;
  const limit = 250;
  slog("info", reqId, `Fetch leads by status ${statusId} (limit=${limit})`);
  while (true) {
    const q = `/api/v4/leads?with=contacts,custom_fields_values&filter[statuses][0][pipeline_id]=${PIPELINE_ID}&filter[statuses][0][status_id]=${statusId}&page=${page}&limit=${limit}`;
    const data  = await kfetch(q, {}, reqId);
    const chunk = data?._embedded?.leads || [];
    slog("debug", reqId, `  page=${page} got=${chunk.length}`);
    all.push(...chunk);
    const next  = data?._links?.next?.href;
    if (!next || chunk.length < limit) break;
    page++;
  }
  slog("info", reqId, `Status ${statusId} total=${all.length}`);
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
  ].filter(Boolean);

  const results = [];
  slog("info", reqId, `Fetching all stages for pipeline ${PIPELINE_ID} (${statuses.length} etapas)`);
  for (const st of statuses) {
    const part = await fetchLeadsByStatus(st, reqId);
    results.push(...part);
  }
  slog("info", reqId, `Pipeline ${PIPELINE_ID} leads aggregated total=${results.length}`);
  return results;
}

async function loadPipelineSnapshotAndIndex(reqId) {
  const allLeads = await fetchAllLeadsForPipeline(reqId);

  const byDoc = new Map();      // doc -> [lead]
  const byEmail = new Map();    // email normalizado -> [lead]
  const byPhone = new Map();    // phone normalizado -> [lead]

  for (const ld of allLeads) {
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
  return { allLeads, byDoc, byEmail, byPhone };
}

function findRelatedLeadsFromIndex({ documento, email, phone }, index) {
  const candidates = new Set();
  if (documento && index.byDoc.has(String(documento))) {
    index.byDoc.get(String(documento)).forEach(ld => candidates.add(ld));
  }
  const ne = (email||"").trim().toLowerCase();
  if (ne && index.byEmail.has(ne)) {
    index.byEmail.get(ne).forEach(ld => candidates.add(ld));
  }
  const np = (phone||"").replace(/[^\d]/g, "");
  if (np && index.byPhone.has(np)) {
    index.byPhone.get(np).forEach(ld => candidates.add(ld));
  }
  return Array.from(candidates);
}

function updateIndexesWithLead(index, lead, { documento, email, phone }) {
  if (!index) return;
  if (documento) {
    const key = String(documento);
    if (!index.byDoc.has(key)) index.byDoc.set(key, []);
    if (!index.byDoc.get(key).some(l => l.id === lead.id)) index.byDoc.get(key).push(lead);
  }
  const ne = normEmail(email);
  if (ne) {
    if (!index.byEmail.has(ne)) index.byEmail.set(ne, []);
    if (!index.byEmail.get(ne).some(l => l.id === lead.id)) index.byEmail.get(ne).push(lead);
  }
  const np = normPhone(phone);
  if (np) {
    if (!index.byPhone.has(np)) index.byPhone.set(np, []);
    if (!index.byPhone.get(np).some(l => l.id === lead.id)) index.byPhone.get(np).push(lead);
  }
}

// ================== CONTACTOS ==================
async function findOrCreateContact({ name, email, phone }, reqId){
  const q = normEmail(email) || normPhone(phone);
  let contact = null;

  if (q) {
    slog("info", reqId, `Buscar contacto por query="${q}"`);
    const cdata = await kfetch(`/api/v4/contacts?query=${encodeURIComponent(q)}`, {}, reqId);
    contact = cdata?._embedded?.contacts?.[0] || null;
    if (contact) {
      slog("info", reqId, `Contacto encontrado id=${contact.id}, name="${contact.name}"`);
    } else {
      slog("info", reqId, `Contacto no encontrado por query`);
    }
  }

  if (!contact) {
    const body = [
      {
        name: name || email || phone || "Sin nombre",
        custom_fields_values: [
          ...(phone ? [{ field_id: CONTACT_CF_PHONE_ID, values: [{ value: normPhone(phone), enum_id: PHONE_ENUM_MOB }] }] : []),
          ...(email ? [{ field_id: CONTACT_CF_EMAIL_ID, values: [{ value: normEmail(email), enum_id: EMAIL_ENUM_WORK }] }] : [])
        ]
      }
    ];
    slog("info", reqId, `Creando nuevo contacto name="${name}" phone="${phone}" email="${email}"`);
    const created = await kfetch(`/api/v4/contacts`, { method: "POST", body: JSON.stringify(body) }, reqId);
    contact = created?._embedded?.contacts?.[0] || created?.[0] || created;
    slog("info", reqId, `Contacto creado id=${contact?.id}`);
    return { contact, created: true, complemented: false };
  }

  // Complementar sin sobrescribir lo existente
  const have = extractContactChannels(contact);
  const patch = { id: contact.id, custom_fields_values: contact.custom_fields_values || [] };
  let complemented = false;

  if (phone && !have.phones.includes(normPhone(phone))) {
    patch.custom_fields_values.push({ field_id: CONTACT_CF_PHONE_ID, values: [{ value: normPhone(phone), enum_id: PHONE_ENUM_MOB }] });
    complemented = true;
    slog("debug", reqId, `Añadiendo teléfono a contacto id=${contact.id}`);
  }
  if (email && !have.emails.includes(normEmail(email))) {
    patch.custom_fields_values.push({ field_id: CONTACT_CF_EMAIL_ID, values: [{ value: normEmail(email), enum_id: EMAIL_ENUM_WORK }] });
    complemented = true;
    slog("debug", reqId, `Añadiendo email a contacto id=${contact.id}`);
  }
  if (name && contact.name !== name) {
    patch.name = name;
    complemented = true;
    slog("debug", reqId, `Actualizando name contacto id=${contact.id} -> "${name}"`);
  }
  if (complemented) {
    slog("info", reqId, `Patch contacto id=${contact.id} (complementar datos)`);
    await kfetch(`/api/v4/contacts`, { method: "PATCH", body: JSON.stringify([patch]) }, reqId);
    const fresh = await kfetch(`/api/v4/contacts/${contact.id}`, {}, reqId);
    contact = fresh;
    slog("info", reqId, `Contacto complementado id=${contact.id}`);
  } else {
    slog("info", reqId, `Contacto id=${contact.id} sin cambios`);
  }
  return { contact, created: false, complemented };
}

// ================== LEADS (crear/actualizar) ==================
async function createLead({ contactId, name, estado, programaEnumId, documento }, reqId){
  const body = [
    {
      name,
      pipeline_id: PIPELINE_ID,
      status_id: estadoToStatusId(estado),
      custom_fields_values: [
        ...(programaEnumId ? [{ field_id: LEAD_CF_PROGRAMA_SINU, values: [{ enum_id: programaEnumId }] }] : []),
        ...(documento ? [{ field_id: LEAD_CF_NUMERO_DOCUMENTO, values: [{ value: String(documento) }] }] : [])
      ],
      _embedded: { contacts: [{ id: contactId }] }
    }
  ];
  slog("info", reqId, `Creando lead (contact_id=${contactId}, estado=${estado}, programaEnumId=${programaEnumId}, doc=${documento})`);
  const data = await kfetch(`/api/v4/leads`, { method: "POST", body: JSON.stringify(body) }, reqId);
  const lead = data?._embedded?.leads?.[0] || data?.[0] || data;
  slog("info", reqId, `Lead creado id=${lead?.id} status_id=${lead?.status_id}`);
  return lead;
}

/**
 * patchLeadStrategy:
 * mode = "advance" -> sólo avanza si target > current (Regla 1)
 * mode = "force"   -> fija la etapa exactamente a target (Regla 2)
 * Además: upsert de programa/documento si faltan.
 */
async function patchLeadStrategy({ lead, targetEstado, programaEnumId, documento, mode = "advance" }, reqId){
  const currentEstado = (() => {
    const sid = lead.status_id;
    if (sid === STATUS.LEADS_ENTRANTES) return "LEADS_ENTRANTES";
    if (sid === STATUS.REASIGNACION)    return "REASIGNACION";
    if (sid === STATUS.INTERESADO)      return "INTERESADO";
    if (sid === STATUS.PREINSCRITO)     return "PREINSCRITO";
    if (sid === STATUS.INSCRITO)        return "INSCRITO";
    if (sid === STATUS.ADMITIDO)        return "ADMITIDO";
    if (sid === STATUS.MATRICULADO)     return "MATRICULADO";
    return "PREINSCRITO";
  })();

  slog("info", reqId, `patchLeadStrategy lead_id=${lead.id} current=${currentEstado} target=${targetEstado} mode=${mode}`);

  const merged = [...(lead.custom_fields_values || [])];

  // upsert documento si no está
  const hasDoc = !!getLeadDocumento(lead);
  if (documento && !hasDoc) {
    merged.push({ field_id: LEAD_CF_NUMERO_DOCUMENTO, values: [{ value: String(documento) }] });
    slog("debug", reqId, `  upsert documento (lead_id=${lead.id}) -> ${documento}`);
  }

  // upsert programa si no está
  const hasProg = !!getLeadProgramaEnumId(lead);
  if (programaEnumId && !hasProg) {
    merged.push({ field_id: LEAD_CF_PROGRAMA_SINU, values: [{ enum_id: programaEnumId }] });
    slog("debug", reqId, `  upsert programa (lead_id=${lead.id}) -> enum_id=${programaEnumId}`);
  }

  // decidir status
  let status_id;
  if (mode === "force") {
    status_id = estadoToStatusId(targetEstado);
    slog("debug", reqId, `  force status -> ${status_id}`);
  } else {
    const shouldAdvance = rank(targetEstado) > rank(currentEstado);
    slog("debug", reqId, `  shouldAdvance=${shouldAdvance} (currentRank=${rank(currentEstado)} targetRank=${rank(targetEstado)})`);
    status_id = shouldAdvance ? estadoToStatusId(targetEstado) : undefined;
  }

  const payload = [{
    id: lead.id,
    ...(status_id ? { status_id } : {}),
    ...(merged.length ? { custom_fields_values: merged } : {})
  }];

  if (!status_id && merged.length === (lead.custom_fields_values||[]).length) {
    slog("info", reqId, `  Nada que actualizar en lead_id=${lead.id}`);
    return {
      from: currentEstado,
      to: currentEstado,
      changedStatus: false,
      upsertedCF: false
    };
  }

  slog("info", reqId, `Patch lead_id=${lead.id} status_id=${status_id ?? "(=)"}, CF+${merged.length - (lead.custom_fields_values||[]).length}`);
  await kfetch(`/api/v4/leads`, { method: "PATCH", body: JSON.stringify(payload) }, reqId);

  // mutate objeto local para que el índice del batch quede consistente
  if (status_id) lead.status_id = status_id;
  lead.custom_fields_values = merged;

  return {
    from: currentEstado,
    to: status_id ? normalizeEstado(targetEstado) : currentEstado,
    changedStatus: Boolean(status_id),
    upsertedCF: merged.length > (lead.custom_fields_values||[]).length
  };
}

// ================== ENDPOINT ÚNICO (BATCH) ==================
app.post("/sync-batch", async (req, res) => {
  const reqId = genReqId();
  try {
    if (!Array.isArray(req.body)) {
      slog("warn", reqId, "Body no es array.");
      return res.status(400).json({ ok:false, error:"El body debe ser un array de objetos" });
    }
    if (req.body.length < 2) {
      slog("warn", reqId, "Array con menos de 2 elementos.");
      return res.status(400).json({ ok:false, error:"Se requiere un array con 2 o más objetos" });
    }

    const items = req.body;
    slog("info", reqId, `>> /sync-batch recibido ${items.length} items`);

    // 1) Snapshot e índices una sola vez
    const snapshot = await loadPipelineSnapshotAndIndex(reqId);
    slog("info", reqId, `Snapshot listo: leads=${snapshot.allLeads.length}`);

    // 2) Concurrencia simple
    const concurrency = Math.min(BATCH_CONCURRENCY, items.length);
    let cursor = 0, inFlight = 0;
    const results = [];
    const runNext = async () => {
      if (cursor >= items.length) return;
      const idx = cursor++;
      inFlight++;

      const inb = items[idx];
      const rid = `${reqId}#${idx}`;
      try {
        // Validación mínima
        if (!isValidItem(inb)) {
          slog("warn", rid, "Item inválido (sin doc/email/phone). Se omite.");
          results[idx] = { ok:false, idx, error:"Faltan doc/email/phone", skipped:true };
        } else {
          // Normalización de entrada
          const documento = (inb.IDENTIFICACION||"").toString().trim();
          const nombre    = normText(inb.NOMBRE||"");
          const email     = normEmail(inb.EMAIL||"");
          const phone     = normPhone(inb.CELULAR||"");
          const estado    = normalizeEstado(inb.ESTADO||"PREINSCRITO");
          const progEnum  = getProgramaEnumId(inb.PROGRAMA);

          slog("info", rid, `Normalizado(batch) doc=${documento} email=${email} phone=${phone} estado=${estado} progEnum=${progEnum}`);

          // 0) Contacto (find/create + complementar)
          const { contact, created: cCreated, complemented } =
            await findOrCreateContact({ name:nombre, email, phone }, rid);

          // 1) Encontrar leads relacionados usando índices
          const related = findRelatedLeadsFromIndex({ documento, email, phone }, snapshot);
          slog("info", rid, `relatedLeads(batch)=${related.length}`);

          // 2) Clasificación por programa
          const sameProgramLead   = related.find(ld => getLeadProgramaEnumId(ld) === progEnum);
          const noProgramLead     = related.find(ld => !getLeadProgramaEnumId(ld));
          const differentProgLead = related.find(ld => {
            const pid = getLeadProgramaEnumId(ld);
            return pid && progEnum && pid !== progEnum;
          });

          // 3) Reglas 1/2/4/3
          if (sameProgramLead) {
            const r = await patchLeadStrategy({
              lead: sameProgramLead,
              targetEstado: estado,
              programaEnumId: progEnum,
              documento
            }, rid);
            updateIndexesWithLead(snapshot, sameProgramLead, { documento, email, phone });
            results[idx] = { ok:true, rule:1, action:r.changedStatus?"lead_advanced":(r.upsertedCF?"lead_upserted":"no_changes"), lead_id:sameProgramLead.id, contact_id:contact.id };
          } else if (noProgramLead) {
            const r = await patchLeadStrategy({
              lead: noProgramLead,
              targetEstado: estado,
              programaEnumId: progEnum,
              documento,
              mode: "force"
            }, rid);
            updateIndexesWithLead(snapshot, noProgramLead, { documento, email, phone });
            results[idx] = { ok:true, rule:2, action:"lead_completed_and_aligned", lead_id:noProgramLead.id, contact_id:contact.id };
          } else if (differentProgLead) {
            const lead = await createLead({
              contactId: contact.id, name: nombre || inb.PROGRAMA || "Lead SGA",
              estado, programaEnumId: progEnum, documento
            }, rid);
            // por si la respuesta no trae status_id
            if (!lead?.status_id) {
              const fresh = await kfetch(`/api/v4/leads/${lead.id}`, {}, rid);
              lead.status_id = fresh?.status_id;
            }
            updateIndexesWithLead(snapshot, lead, { documento, email, phone });
            results[idx] = { ok:true, rule:4, action:"lead_created_new_program", lead_id:lead.id, contact_id:contact.id, status_id:lead.status_id };
          } else {
            const lead = await createLead({
              contactId: contact.id, name: nombre || inb.PROGRAMA || "Lead SGA",
              estado, programaEnumId: progEnum, documento
            }, rid);
            if (!lead?.status_id) {
              const fresh = await kfetch(`/api/v4/leads/${lead.id}`, {}, rid);
              lead.status_id = fresh?.status_id;
            }
            updateIndexesWithLead(snapshot, lead, { documento, email, phone });
            results[idx] = { ok:true, rule:3, action:"lead_created", lead_id:lead.id, contact_id:contact.id, status_id:lead.status_id };
          }
        }
      } catch (e) {
        slog("error", rid, "ERROR item:", e?.message || e);
        results[idx] = { ok:false, idx, error: e?.message || String(e) };
      } finally {
        inFlight--;
        if (cursor < items.length) runNext();
      }
    };

    // Arranca N workers
    for (let i=0;i<concurrency;i++) runNext();

    // Espera a que terminen
    const waitAll = async () => { while (inFlight > 0 || cursor < items.length) await new Promise(r => setTimeout(r, 50)); };
    await waitAll();

    // Resumen
    const ok = results.filter(r=>r?.ok).length;
    const created = results.filter(r=>r?.action?.startsWith("lead_created")).length;
    const advanced = results.filter(r=>r?.action==="lead_advanced").length;
    const upserted = results.filter(r=>r?.action==="lead_upserted" || r?.action==="lead_completed_and_aligned").length;
    const skipped  = results.filter(r=>r?.skipped).length;

    slog("info", reqId, `Batch done: ok=${ok}/${items.length} created=${created} advanced=${advanced} upserted=${upserted} skipped=${skipped}`);
    return res.json({ ok:true, total: items.length, created, advanced, upserted, skipped, results });

  } catch (err) {
    slog("error", reqId, "sync-batch ERROR:", err?.message || err);
    return res.status(500).json({ ok:false, error: err.message || String(err), reqId });
  }
});

// healthcheck
app.get("/health", (req, res) => {
  const reqId = genReqId();
  slog("info", reqId, `Healthcheck OK pipeline=${PIPELINE_ID} base=${BASE}`);
  res.json({ ok: true, pipeline: PIPELINE_ID, base: BASE });
});

app.listen(PORT, ()=> console.log(`[BOOT] Sync API (idempotente) en :${PORT} base=${BASE} log=${LOG_LEVEL} conc=${BATCH_CONCURRENCY}`));
