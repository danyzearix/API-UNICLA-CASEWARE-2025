import express from "express";
import dotenv from 'dotenv';
dotenv.config();

// ================== ENV ==================
const BASE  = process.env.KOMMO_BASE_URL;   // ej: https://crmuniclaretiana.kommo.com
const TOKEN = process.env.KOMMO_TOKEN;      // ej: Bearer eyJ0eXAi...
const PORT  = process.env.PORT || 3000;
const LOG_LEVEL = (process.env.LOG_LEVEL || "info").toLowerCase();

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
  INTERESADO:      89977000, // agregado
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

// ================== HELPERS ==================
const app = express();
app.use(express.json());

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

// ================== CARGA TOTAL DE LEADS DEL PIPELINE ==================
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

  return {
    from: currentEstado,
    to: status_id ? normalizeEstado(targetEstado) : currentEstado,
    changedStatus: Boolean(status_id),
    upsertedCF: merged.length > (lead.custom_fields_values||[]).length
  };
}

// ================== ENDPOINT ÚNICO ==================
/*
Input ejemplo:
{
  "IDENTIFICACION": "1010118868",
  "NOMBRE": "GUERRERO RIVERA JOSÉ DAVID",
  "EMAIL": "JGUERRERO2411@GMAIL.COM",
  "CELULAR": "3043253947",
  "ESTADO": "INSCRITO",
  "PROGRAMA": "EEB01  ESPECIALIZACIÓN EN ESTUDIOS BÍBLICOS"
}
*/
app.post("/sync-one", async (req, res) => {
  const reqId = genReqId();
  try {
    slog("info", reqId, ">> /sync-one recibido", "body:", req.body);

    const inb       = req.body || {};
    const documento = (inb.IDENTIFICACION||"").toString().trim();
    const nombre    = normText(inb.NOMBRE||"");
    const email     = normEmail(inb.EMAIL);
    const phone     = normPhone(inb.CELULAR);
    const estado    = normalizeEstado(inb.ESTADO||"PREINSCRITO");
    const progEnum  = getProgramaEnumId(inb.PROGRAMA);

    slog("info", reqId, `Normalizado -> doc=${documento} nombre="${nombre}" email="${email}" phone="${phone}" estado=${estado} progEnum=${progEnum}`);

    // 0) Contacto (find/create + completar canales)
    const { contact, created: contactCreated, complemented } =
      await findOrCreateContact({ name: nombre, email, phone }, reqId);
    slog("info", reqId, `Contacto listo id=${contact.id} created=${contactCreated} complemented=${complemented}`);

    // 1) Descargar TODOS los leads del pipeline y buscar por contacto/doc
    const allLeads = await fetchAllLeadsForPipeline(reqId);
    slog("info", reqId, `Leads totales cargados=${allLeads.length}`);

    const relatedLeads = allLeads.filter(ld => {
      const docOk = documento && getLeadDocumento(ld) === String(documento);

      let contactOk = false;
      const c = ld._embedded?.contacts?.[0];
      if (c) {
        const ch = extractContactChannels(c);
        if (email && ch.emails.includes(email)) contactOk = true;
        if (phone && ch.phones.includes(phone)) contactOk = true;
      }
      return docOk || contactOk;
    });

    slog("info", reqId, `relatedLeads encontrados=${relatedLeads.length}`);

    // Clasificar por programa
    const sameProgramLead   = relatedLeads.find(ld => getLeadProgramaEnumId(ld) === progEnum);
    const noProgramLead     = relatedLeads.find(ld => !getLeadProgramaEnumId(ld));
    const differentProgLead = relatedLeads.find(ld => {
      const pid = getLeadProgramaEnumId(ld);
      return pid && progEnum && pid !== progEnum;
    });

    slog("debug", reqId, `sameProgramLead=${sameProgramLead?.id || null} noProgramLead=${noProgramLead?.id || null} differentProgLead=${differentProgLead?.id || null}`);

    // ========== Reglas ==========
    // Regla 1 — Mismo programa + etapa JSON superior: advance
    if (sameProgramLead) {
      slog("info", reqId, `Regla 1: actualizar/avanzar lead existente id=${sameProgramLead.id}`);
      const result = await patchLeadStrategy({
        lead: sameProgramLead,
        targetEstado: estado,
        programaEnumId: progEnum,
        documento
      }, reqId);
      const payload = {
        ok: true,
        rule: 1,
        action: result.changedStatus ? "lead_advanced" : (result.upsertedCF ? "lead_upserted" : "no_changes"),
        lead_id: sameProgramLead.id,
        contact_id: contact.id,
        contact_created: contactCreated,
        contact_complemented: complemented,
        ...result
      };
      slog("info", reqId, `Salida Regla 1 ->`, payload);
      return res.json(payload);
    }

    // Regla 2 — Lead existe pero sin programa: forzar etapa + completar datos
    if (noProgramLead) {
      slog("info", reqId, `Regla 2: completar lead sin programa id=${noProgramLead.id}`);
      const result = await patchLeadStrategy({
        lead: noProgramLead,
        targetEstado: estado,
        programaEnumId: progEnum,
        documento,
        mode: "force"
      }, reqId);
      const payload = {
        ok: true,
        rule: 2,
        action: "lead_completed_and_aligned",
        lead_id: noProgramLead.id,
        contact_id: contact.id,
        contact_created: contactCreated,
        contact_complemented: complemented,
        ...result
      };
      slog("info", reqId, `Salida Regla 2 ->`, payload);
      return res.json(payload);
    }

    // Regla 4 — Lead con programa distinto: crear nuevo lead
    if (differentProgLead) {
      slog("info", reqId, `Regla 4: crear lead nuevo por programa distinto (contact_id=${contact.id})`);
      const createdLead = await createLead({
        contactId: contact.id,
        name: nombre || inb.PROGRAMA || "Lead SGA",
        estado,
        programaEnumId: progEnum,
        documento
      }, reqId);
      const payload = {
        ok: true,
        rule: 4,
        action: "lead_created_new_program",
        lead_id: createdLead.id,
        contact_id: contact.id,
        contact_created: contactCreated,
        contact_complemented: complemented
      };
      slog("info", reqId, `Salida Regla 4 ->`, payload);
      return res.json(payload);
    }

    // Regla 3 — No hay lead relacionado: crear lead
    slog("info", reqId, `Regla 3: crear lead nuevo (contact_id=${contact.id})`);
    const createdLead = await createLead({
      contactId: contact.id,
      name: nombre || inb.PROGRAMA || "Lead SGA",
      estado,
      programaEnumId: progEnum,
      documento
    }, reqId);

    const payload = {
      ok: true,
      rule: 3,
      action: "lead_created",
      lead_id: createdLead.id,
      contact_id: contact.id,
      contact_created: contactCreated,
      contact_complemented: complemented
    };
    slog("info", reqId, `Salida Regla 3 ->`, payload);
    return res.json(payload);

  } catch (err) {
    slog("error", reqId, "sync-one ERROR:", err?.message || err);
    return res.status(500).json({ ok:false, error: err.message || String(err), reqId });
  }
});

// healthcheck
app.get("/health", (req, res) => {
  const reqId = genReqId();
  slog("info", reqId, `Healthcheck OK pipeline=${PIPELINE_ID} base=${BASE}`);
  res.json({ ok: true, pipeline: PIPELINE_ID, base: BASE });
});

app.listen(PORT, ()=> console.log(`[BOOT] Sync API (idempotente) en :${PORT} base=${BASE} log=${LOG_LEVEL}`));

