// ╔══════════════════════════════════════════════════════════════════╗
// ║  LibroDiario — Sistema Contable Integrado                        ║
// ║  Supabase + Google/GitHub Auth                                   ║
// ╚══════════════════════════════════════════════════════════════════╝
import { useState, useEffect, useMemo } from "react";
import { createClient } from "@supabase/supabase-js";

// ── Supabase client ──
const sb = createClient(
  import.meta.env.VITE_SUPABASE_URL,
  import.meta.env.VITE_SUPABASE_ANON_KEY
);

// ── Helpers ──
function genId() { return crypto.randomUUID(); }
function today() { return new Date().toISOString().slice(0,10); }
function lsLoad(k,fb){ try{const v=localStorage.getItem(k);return v?JSON.parse(v):fb;}catch{return fb;} }
function lsSave(k,v){ try{localStorage.setItem(k,JSON.stringify(v));}catch{} }

// ── Entities (multi-empresa) ──
async function dbLoadEntities(uid){
  if(!uid) return lsLoad("ac_entities",[]);
  const {data,error}=await sb.from("ac_entities").select("*").eq("user_id",uid).order("name");
  if(error){console.error(error);return lsLoad("ac_entities",[]);}
  const rows=(data||[]).map(r=>({id:r.id,rut:r.rut,name:r.name,giro:r.giro||"",createdAt:r.created_at}));
  lsSave("ac_entities",rows); return rows;
}
async function dbUpsertEntity(uid,e,all){
  lsSave("ac_entities",all);
  if(!uid) return;
  const {error}=await sb.from("ac_entities").upsert({id:e.id,user_id:uid,rut:e.rut,name:e.name,giro:e.giro||null},{onConflict:"id"});
  if(error) console.error("upsert entity",error);
}
async function dbDeleteEntity(uid,id,all){
  lsSave("ac_entities",all);
  if(!uid) return;
  const {error}=await sb.from("ac_entities").delete().eq("id",id).eq("user_id",uid);
  if(error) console.error("delete entity",error);
}

// ── Supabase CRUD (jsonb strategy — entity-aware) ──
async function dbLoad(table, uid, eid, cacheKey, fallback){
  const key=cacheKey+(eid?":"+eid:"");
  if(!uid) return lsLoad(key, fallback);
  let q=sb.from(table).select("id,data").eq("user_id",uid);
  if(eid) q=q.eq("entity_id",eid);
  const {data,error}=await q;
  if(error){console.error(table,error);return lsLoad(key,fallback);}
  const rows=(data||[]).map(r=>({...r.data,id:r.id}));
  lsSave(key,rows); return rows;
}
async function dbUpsert(table, uid, eid, record, cacheKey, all){
  const key=cacheKey+(eid?":"+eid:"");
  lsSave(key,all);
  if(!uid) return;
  const payload={id:record.id,user_id:uid,data:record};
  if(eid) payload.entity_id=eid;
  const {error}=await sb.from(table).upsert(payload,{onConflict:"id"});
  if(error) console.error("upsert",table,error);
}
async function dbDelete(table, uid, id, cacheKey, eid, all){
  const key=cacheKey+(eid?":"+eid:"");
  lsSave(key,all);
  if(!uid) return;
  const {error}=await sb.from(table).delete().eq("id",id).eq("user_id",uid);
  if(error) console.error("delete",table,error);
}

// ── Entries (explicit columns — entity-aware) ──
async function dbLoadEntries(uid, eid){
  const key="ac_entries"+(eid?":"+eid:"");
  if(!uid) return lsLoad(key,[]);
  let q=sb.from("ac_entries").select("*").eq("user_id",uid);
  if(eid) q=q.eq("entity_id",eid);
  const {data,error}=await q.order("number");
  if(error){console.error(error);return lsLoad(key,[]);}
  const rows=(data||[]).map(r=>({id:r.id,number:r.number,date:r.date,description:r.description,reference:r.reference,rows:r.rows,totalDebit:+r.total_debit,totalCredit:+r.total_credit,createdAt:r.created_at}));
  lsSave(key,rows); return rows;
}
async function dbUpsertEntry(uid, eid, e, all){
  const key="ac_entries"+(eid?":"+eid:"");
  lsSave(key,all);
  if(!uid) return;
  const payload={id:e.id,user_id:uid,number:e.number,date:e.date,description:e.description,reference:e.reference||null,rows:e.rows,total_debit:e.totalDebit,total_credit:e.totalCredit};
  if(eid) payload.entity_id=eid;
  const {error}=await sb.from("ac_entries").upsert(payload,{onConflict:"id"});
  if(error) console.error("upsert entry",error);
}
async function dbDeleteEntry(uid, id, eid, all){
  const key="ac_entries"+(eid?":"+eid:"");
  lsSave(key,all);
  if(!uid) return;
  const {error}=await sb.from("ac_entries").delete().eq("id",id).eq("user_id",uid);
  if(error) console.error("delete entry",error);
}

// ── Accounts (explicit columns — entity-aware) ──
async function dbLoadAccounts(uid, eid){
  const key="ac_accounts"+(eid?":"+eid:"");
  if(!uid) return lsLoad(key, DEFAULT_ACCOUNTS);
  let q=sb.from("ac_accounts").select("*").eq("user_id",uid);
  if(eid) q=q.eq("entity_id",eid);
  const {data,error}=await q.order("code");
  if(error){console.error(error);return lsLoad(key,DEFAULT_ACCOUNTS);}
  if(!data||data.length===0){
    const rows=DEFAULT_ACCOUNTS.map(a=>({id:genId(),user_id:uid,entity_id:eid||null,code:a.code,name:a.name,type:a.type}));
    await sb.from("ac_accounts").insert(rows);
    return DEFAULT_ACCOUNTS;
  }
  const rows=data.map(r=>({code:r.code,name:r.name,type:r.type}));
  lsSave(key,rows); return rows;
}
async function dbUpsertAccount(uid, eid, a, all){
  const key="ac_accounts"+(eid?":"+eid:"");
  lsSave(key,all);
  if(!uid) return;
  const {error}=await sb.from("ac_accounts").upsert({id:genId(),user_id:uid,entity_id:eid||null,code:a.code,name:a.name,type:a.type},{onConflict:"user_id,code"});
  if(error) console.error("upsert account",error);
}
async function dbDeleteAccount(uid, code, all){
  lsSave("ac_accounts",all);
  if(!uid) return;
  const {error}=await sb.from("ac_accounts").delete().eq("user_id",uid).eq("code",code);
  if(error) console.error("delete account",error);
}


// ════════════════════════════════════════════════════════════════
//  AUTH HOOK + LOGIN SCREEN
// ════════════════════════════════════════════════════════════════
function useAuth(){
  const [user,setUser]=useState(null);
  const [loading,setLoading]=useState(true);
  useEffect(()=>{
    sb.auth.getSession().then(({data:{session}})=>{setUser(session?.user??null);setLoading(false);});
    const {data:{subscription}}=sb.auth.onAuthStateChange((_,session)=>setUser(session?.user??null));
    return ()=>subscription.unsubscribe();
  },[]);
  const signInGoogle=()=>sb.auth.signInWithOAuth({provider:"google",options:{redirectTo:window.location.origin}});
  const signInGitHub=()=>sb.auth.signInWithOAuth({provider:"github",options:{redirectTo:window.location.origin}});
  const signOut=()=>sb.auth.signOut();
  return{user,loading,signInGoogle,signInGitHub,signOut};
}

function LoginScreen({signInGoogle,signInGitHub}){
  return(
    <div style={{fontFamily:"'Georgia',serif",background:"#0f172a",minHeight:"100vh",display:"flex",alignItems:"center",justifyContent:"center"}}>
      <div style={{background:"#fff",borderRadius:6,padding:"48px 56px",maxWidth:400,width:"100%",textAlign:"center",boxShadow:"0 24px 48px rgba(0,0,0,.4)"}}>
        <div style={{fontSize:40,marginBottom:10}}>⚖</div>
        <div style={{fontSize:26,fontWeight:700,color:"#0f172a",marginBottom:4}}>LibroDiario</div>
        <div style={{fontSize:10,color:"#64748b",letterSpacing:3,textTransform:"uppercase",marginBottom:36}}>Sistema Contable Integrado</div>
        <div style={{display:"flex",flexDirection:"column",gap:12}}>
          <button onClick={signInGoogle} style={{display:"flex",alignItems:"center",justifyContent:"center",gap:12,background:"#fff",border:"1px solid #e2e8f0",borderRadius:4,padding:"12px 20px",fontSize:14,fontFamily:"'Georgia',serif",cursor:"pointer",color:"#0f172a",fontWeight:600,boxShadow:"0 1px 3px rgba(0,0,0,.08)"}}>
            <svg width="18" height="18" viewBox="0 0 24 24"><path fill="#4285F4" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"/><path fill="#34A853" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"/><path fill="#FBBC05" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.84z"/><path fill="#EA4335" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"/></svg>
            Continuar con Google
          </button>
          <button onClick={signInGitHub} style={{display:"flex",alignItems:"center",justifyContent:"center",gap:12,background:"#24292e",border:"none",borderRadius:4,padding:"12px 20px",fontSize:14,fontFamily:"'Georgia',serif",cursor:"pointer",color:"#fff",fontWeight:600}}>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="white"><path d="M12 0C5.37 0 0 5.37 0 12c0 5.31 3.435 9.795 8.205 11.385.6.105.825-.255.825-.57 0-.285-.015-1.23-.015-2.235-3.015.555-3.795-.735-4.035-1.41-.135-.345-.72-1.41-1.23-1.695-.42-.225-1.02-.78-.015-.795.945-.015 1.62.87 1.845 1.23 1.08 1.815 2.805 1.305 3.495.99.105-.78.42-1.305.765-1.605-2.67-.3-5.46-1.335-5.46-5.925 0-1.305.465-2.385 1.23-3.225-.12-.3-.54-1.53.12-3.18 0 0 1.005-.315 3.3 1.23.96-.27 1.98-.405 3-.405s2.04.135 3 .405c2.295-1.56 3.3-1.23 3.3-1.23.66 1.65.24 2.88.12 3.18.765.84 1.23 1.905 1.23 3.225 0 4.605-2.805 5.625-5.475 5.925.435.375.81 1.095.81 2.22 0 1.605-.015 2.895-.015 3.3 0 .315.225.69.825.57A12.02 12.02 0 0024 12c0-6.63-5.37-12-12-12z"/></svg>
            Continuar con GitHub
          </button>
        </div>
        <p style={{fontSize:11,color:"#94a3b8",marginTop:28,lineHeight:1.7}}>Tus datos se guardan de forma segura.<br/>Solo tú tienes acceso a tu información.</p>
      </div>
    </div>
  );
}

const fmtCLP  = n => new Intl.NumberFormat("es-CL",{style:"currency",currency:"CLP",minimumFractionDigits:0}).format(n||0);
const fmtNum  = (n,d=2) => new Intl.NumberFormat("es-CL",{minimumFractionDigits:d,maximumFractionDigits:d}).format(n||0);
const fmtDate = d => d ? new Date(d+"T12:00:00").toLocaleDateString("es-CL",{day:"2-digit",month:"2-digit",year:"numeric"}) : "—";
function addMonths(ds, n) { const d = new Date(ds+"T12:00:00"); d.setMonth(d.getMonth()+n); return d.toISOString().slice(0,10); }
function toCLP(amount, currency, rates) { return currency==="CLP" ? amount : amount*(rates[currency]||1); }

// ════════════════════════════════════════════════════════════════
//  COLORS & STYLES
// ════════════════════════════════════════════════════════════════
const C = { navy:"#0f172a", gold:"#b8973f", cream:"#f8f6f1", muted:"#64748b", border:"#e2e8f0",
  danger:"#dc2626", green:"#15803d", greenBg:"#f0fdf4", redBg:"#fef2f2", blue:"#1d4ed8",
  amber:"#b45309", purple:"#7c3aed" };

const S = {
  app:   { fontFamily:"'Georgia',serif", background:C.cream, minHeight:"100vh", color:C.navy },
  topBar:{ background:C.navy, color:"#fff", padding:"0 32px", display:"flex", alignItems:"center", justifyContent:"space-between", height:60, borderBottom:`3px solid ${C.gold}` },
  logo:  { fontFamily:"'Georgia',serif", fontSize:20, fontWeight:700, letterSpacing:1, color:"#fff" },
  logosub: { fontSize:10, color:C.gold, letterSpacing:3, textTransform:"uppercase" },
  fxBar: { background:"#1e293b", color:"#fff", padding:"6px 32px", display:"flex", alignItems:"center", gap:20, fontSize:12, flexWrap:"wrap", borderBottom:"1px solid #334155" },
  sectBar:{ display:"flex", gap:0, background:"#fff", borderBottom:`2px solid ${C.border}`, padding:"0 32px", overflowX:"auto" },
  sectTab:(a)=>({ background:"none", border:"none", borderBottom:a?`3px solid ${C.gold}`:"3px solid transparent",
    color:a?C.navy:C.muted, padding:"13px 20px", cursor:"pointer", fontFamily:"'Georgia',serif",
    fontSize:12.5, fontWeight:a?700:400, letterSpacing:1, textTransform:"uppercase", whiteSpace:"nowrap", marginBottom:-2 }),
  subBar:{ display:"flex", gap:0, background:"#f8fafc", borderBottom:`1px solid ${C.border}`, padding:"0 32px" },
  subTab:(a)=>({ background:"none", border:"none", borderBottom:a?`2px solid ${C.gold}`:"2px solid transparent",
    color:a?C.navy:C.muted, padding:"9px 16px", cursor:"pointer", fontFamily:"'Georgia',serif",
    fontSize:11.5, fontWeight:a?600:400, letterSpacing:.5, textTransform:"uppercase", whiteSpace:"nowrap", marginBottom:-1 }),
  body:  { maxWidth:1300, margin:"0 auto", padding:"28px 24px" },
  card:  { background:"#fff", border:`1px solid ${C.border}`, borderRadius:4, marginBottom:20, overflow:"hidden", boxShadow:"0 1px 6px rgba(0,0,0,.05)" },
  cHead: (bg=C.navy)=>({ background:bg, padding:"12px 20px", display:"flex", alignItems:"center", justifyContent:"space-between" }),
  cTitle:{ fontFamily:"'Georgia',serif", fontSize:12.5, fontWeight:700, letterSpacing:2, textTransform:"uppercase", color:C.gold },
  cBody: { padding:22 },
  label: { display:"block", fontSize:10.5, fontWeight:700, letterSpacing:2, textTransform:"uppercase", color:C.muted, marginBottom:5 },
  input: { width:"100%", border:`1px solid ${C.border}`, borderRadius:3, padding:"9px 11px", fontSize:13.5, fontFamily:"'Georgia',serif", color:C.navy, background:"#fafaf9", boxSizing:"border-box", outline:"none" },
  select:{ width:"100%", border:`1px solid ${C.border}`, borderRadius:3, padding:"9px 11px", fontSize:13.5, fontFamily:"'Georgia',serif", color:C.navy, background:"#fafaf9", boxSizing:"border-box", outline:"none" },
  textarea:{ width:"100%", border:`1px solid ${C.border}`, borderRadius:3, padding:"9px 11px", fontSize:13, fontFamily:"'Georgia',serif", color:C.navy, background:"#fafaf9", boxSizing:"border-box", outline:"none", resize:"vertical", minHeight:68 },
  g2:{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:16 },
  g3:{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr", gap:16 },
  g4:{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr 1fr", gap:14 },
  g5:{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr 1fr 1fr", gap:14 },
  btn:(bg,fg,br)=>({ background:bg, color:fg, border:`2px solid ${br||bg}`, borderRadius:3, padding:"8px 18px", fontSize:10.5, fontWeight:700, letterSpacing:2, textTransform:"uppercase", cursor:"pointer", fontFamily:"'Georgia',serif", whiteSpace:"nowrap" }),
  bsm:(bg,fg)=>({ background:bg, color:fg, border:`1px solid ${bg==="transparent"?C.border:bg}`, borderRadius:3, padding:"4px 10px", fontSize:10, fontWeight:700, letterSpacing:1, textTransform:"uppercase", cursor:"pointer", fontFamily:"'Georgia',serif" }),
  th:  { background:C.navy, color:C.gold, padding:"8px 12px", fontSize:10, letterSpacing:2, textTransform:"uppercase", fontFamily:"'Georgia',serif", textAlign:"left", whiteSpace:"nowrap" },
  td:  { padding:"8px 12px", borderBottom:`1px solid ${C.border}`, fontSize:12.5, verticalAlign:"middle" },
  stat:{ background:"#fff", border:`1px solid ${C.border}`, borderRadius:4, padding:"16px 20px", borderTop:`3px solid ${C.gold}` },
  sLbl:{ fontSize:9.5, letterSpacing:2.5, textTransform:"uppercase", color:C.muted, marginBottom:6 },
  sVal:{ fontSize:20, fontWeight:700, fontFamily:"'Georgia',serif", color:C.navy },
  divider:{ border:"none", borderTop:`1px solid ${C.border}`, margin:"16px 0" },
  empty:{ textAlign:"center", padding:"40px 24px", color:C.muted },
  pill:(ok)=>({ background:ok?"#dcfce7":"#fef9c3", color:ok?C.green:"#92400e", padding:"2px 8px", borderRadius:20, fontSize:9.5, fontWeight:700, letterSpacing:1, textTransform:"uppercase" }),
  alert:(ok)=>({ background:ok?C.greenBg:C.redBg, border:`1px solid ${ok?C.green:C.danger}`, borderRadius:3, padding:"9px 14px", fontSize:12.5, color:ok?C.green:C.danger, marginBottom:14 }),
  tag:(t)=>{
    const m={Activo:["#dbeafe",C.blue],Pasivo:["#fce7f3","#9d174d"],Patrimonio:["#ede9fe",C.purple],
      Ingreso:["#dcfce7",C.green],Gasto:["#fef2f2",C.danger],
      frances:["#dbeafe",C.blue],aleman:["#dcfce7",C.green],bullet:["#fce7f3","#9d174d"],personalizado:["#fef3c7","#92400e"],
      Acción:["#dcfce7",C.green],Bono:["#dbeafe",C.blue],Fondo:["#ede9fe",C.purple],ETF:["#fef9c3",C.amber],Otro:["#f1f5f9",C.muted],
      Inventario:["#dcfce7",C.green],"Activo Fijo":["#fef9c3",C.amber]};
    const [bg,fg]=m[t]||["#f1f5f9",C.muted];
    return{background:bg,color:fg,padding:"2px 7px",borderRadius:20,fontSize:9.5,fontWeight:700,letterSpacing:1,textTransform:"uppercase",display:"inline-block"};
  },
};

// ════════════════════════════════════════════════════════════════
//  BASE COMPONENTS
// ════════════════════════════════════════════════════════════════
const Field = ({label,children,style})=><div style={style}><label style={S.label}>{label}</label>{children}</div>;
const Inp   = ({label,...p})=><Field label={label}><input style={S.input} {...p}/></Field>;
const Sel   = ({label,options,...p})=><Field label={label}><select style={S.select} {...p}>{options.map(o=>typeof o==="string"?<option key={o}>{o}</option>:<option key={o.v} value={o.v}>{o.l}</option>)}</select></Field>;
const Btn   = ({children,onClick,v="primary",sm,disabled,style})=>{
  const styles={primary:S.btn(C.navy,C.gold),gold:S.btn(C.gold,C.navy),outline:S.btn("transparent",C.navy,C.border),danger:S.btn("transparent",C.danger,C.danger)};
  return <button style={{...styles[v],...(sm?{padding:"6px 12px",fontSize:9.5}:{}),...(disabled?{opacity:.45,cursor:"default"}:{}),...(style||{})}} onClick={onClick} disabled={disabled}>{children}</button>;
};
const Msg   = ({ok,children})=>children?<div style={S.alert(ok)}>{children}</div>:null;

function StatGrid({stats}){
  return <div style={{display:"grid",gridTemplateColumns:`repeat(${stats.length},1fr)`,gap:14,marginBottom:22}}>
    {stats.map((s,i)=><div key={i} style={{...S.stat,borderTopColor:s.danger?C.danger:s.green?C.green:C.gold}}>
      <div style={S.sLbl}>{s.label}</div>
      <div style={{...S.sVal,fontSize:s.small?15:20,color:s.danger?C.danger:s.green?C.green:C.navy}}>{s.value}</div>
      {s.sub&&<div style={{fontSize:10.5,color:C.muted,marginTop:3}}>{s.sub}</div>}
    </div>)}
  </div>;
}

function DataTable({cols,rows,emptyMsg="Sin registros"}){
  if(!rows||!rows.length) return <div style={S.empty}><div style={{fontSize:32,marginBottom:8}}>📋</div><div style={{fontFamily:"'Georgia',serif",color:C.muted}}>{emptyMsg}</div></div>;
  return <div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12.5}}>
    <thead><tr>{cols.map((c,i)=><th key={i} style={{...S.th,textAlign:c.r?"right":"left"}}>{c.label}</th>)}</tr></thead>
    <tbody>{rows.map((row,ri)=><tr key={ri} style={{background:ri%2===0?"#fafaf9":"#fff"}}>
      {cols.map((c,ci)=><td key={ci} style={{...S.td,textAlign:c.r?"right":"left"}}>{c.fn?c.fn(row,ri):row[c.key]}</td>)}
    </tr>)}</tbody>
  </table></div>;
}

// ════════════════════════════════════════════════════════════════
//  FX RATES — AUTO via mindicador.cl
// ════════════════════════════════════════════════════════════════
function useFxRates() {
  const INDICATORS = { UF:"uf", USD:"dolar", EUR:"euro", UTM:"utm" };
  const [rates, setRates]  = useState(()=>lsLoad("ac_fx",{UF:37500,USD:950,EUR:1030,UTM:65000}));
  const [meta,  setMeta]   = useState(()=>lsLoad("ac_fx_meta",{date:null,loading:false,error:null}));

  async function fetchRates() {
    setMeta(m=>({...m,loading:true,error:null}));
    const newRates = {...rates};
    let anyOk = false;
    for(const [key,ind] of Object.entries(INDICATORS)){
      try {
        const res = await fetch(`https://mindicador.cl/api/${ind}`);
        if(!res.ok) throw new Error("HTTP "+res.status);
        const data = await res.json();
        const val = data.serie?.[0]?.valor;
        if(val){ newRates[key]=val; anyOk=true; }
      } catch(e){ /* silently skip individual failures */ }
    }
    if(anyOk){
      setRates(newRates);
      lsSave("ac_fx", newRates);
      const d = today();
      setMeta({date:d,loading:false,error:null});
      lsSave("ac_fx_meta",{date:d,loading:false,error:null});
    } else {
      setMeta(m=>({...m,loading:false,error:"No se pudo conectar con mindicador.cl"}));
    }
  }

  // Auto-fetch once per day
  useEffect(()=>{
    const lastDate = lsLoad("ac_fx_meta",{}).date;
    if(lastDate !== today()) fetchRates();
  },[]);

  return { rates, meta, fetchRates };
}

function FxBar({rates,meta,fetchRates}){
  return <div style={S.fxBar}>
    <span style={{color:C.gold,fontWeight:700,letterSpacing:2,textTransform:"uppercase",fontSize:9.5,whiteSpace:"nowrap"}}>Indicadores</span>
    {[
      {k:"UF",  label:"UF"},
      {k:"USD", label:"Dólar"},
      {k:"EUR", label:"Euro"},
      {k:"UTM", label:"UTM"},
    ].map(({k,label})=>(
      <span key={k} style={{color:"#94a3b8",fontSize:11.5,letterSpacing:.3}}>
        <span style={{color:"#cbd5e1",fontWeight:700}}>{label}</span>{" "}
        {fmtCLP(rates[k]||0)}
      </span>
    ))}
    <span style={{marginLeft:"auto",display:"flex",alignItems:"center",gap:10}}>
      {meta.error && <span style={{color:"#f87171",fontSize:10}}>{meta.error}</span>}
      {meta.date  && !meta.loading && <span style={{color:"#475569",fontSize:10}}>Fuente: mindicador.cl · {fmtDate(meta.date)}</span>}
      {meta.loading
        ? <span style={{color:C.gold,fontSize:10,letterSpacing:1}}>Actualizando…</span>
        : <button onClick={fetchRates} style={{...S.bsm("transparent",C.gold),border:`1px solid ${C.gold}`}}>↻ Actualizar</button>}
    </span>
  </div>;
}


// ════════════════════════════════════════════════════════════════
//  ACCOUNTING — PLAN DE CUENTAS & ASIENTOS
// ════════════════════════════════════════════════════════════════
const DEFAULT_ACCOUNTS = [
  {code:"1100",name:"Caja",type:"Activo"},{code:"1110",name:"Banco Cuenta Corriente",type:"Activo"},
  {code:"1120",name:"Banco Cuenta Ahorro",type:"Activo"},{code:"1200",name:"Cuentas por Cobrar Clientes",type:"Activo"},
  {code:"1210",name:"Documentos por Cobrar",type:"Activo"},{code:"1300",name:"Inventario de Mercaderías",type:"Activo"},
  {code:"1400",name:"IVA Crédito Fiscal",type:"Activo"},{code:"1500",name:"Activo Fijo - Maquinaria",type:"Activo"},
  {code:"1510",name:"Activo Fijo - Vehículos",type:"Activo"},{code:"1520",name:"Deprec. Acumulada Maquinaria",type:"Activo"},
  {code:"2100",name:"Cuentas por Pagar Proveedores",type:"Pasivo"},{code:"2110",name:"Documentos por Pagar",type:"Pasivo"},
  {code:"2200",name:"IVA Débito Fiscal",type:"Pasivo"},{code:"2300",name:"Impuesto por Pagar",type:"Pasivo"},
  {code:"2400",name:"Préstamo Bancario LP",type:"Pasivo"},{code:"2440",name:"Reajuste UF Deuda",type:"Pasivo"},
  {code:"3100",name:"Capital Social",type:"Patrimonio"},{code:"3200",name:"Utilidades Retenidas",type:"Patrimonio"},
  {code:"3300",name:"Resultado del Ejercicio",type:"Patrimonio"},
  {code:"4100",name:"Ingresos por Ventas",type:"Ingreso"},{code:"4110",name:"Otros Ingresos",type:"Ingreso"},
  {code:"5100",name:"Costo de Ventas",type:"Gasto"},{code:"5200",name:"Gastos de Remuneraciones",type:"Gasto"},
  {code:"5210",name:"Gastos de Arriendo",type:"Gasto"},{code:"5220",name:"Gastos Servicios Básicos",type:"Gasto"},
  {code:"5230",name:"Gastos de Depreciación",type:"Gasto"},{code:"5240",name:"Gastos Financieros",type:"Gasto"},
  {code:"5241",name:"Reajuste UF",type:"Gasto"},{code:"5250",name:"Gastos Administrativos",type:"Gasto"},
  {code:"1600",name:"Inversiones en Acciones",type:"Activo"},{code:"1610",name:"Inversiones en Bonos",type:"Activo"},
  {code:"1620",name:"Inversiones en Fondos/ETF",type:"Activo"},{code:"1630",name:"Otros Instrumentos Financieros",type:"Activo"},
  {code:"4200",name:"Dividendos Recibidos",type:"Ingreso"},{code:"4300",name:"Ganancia en Inversiones",type:"Ingreso"},
  {code:"4400",name:"Intereses Ganados",type:"Ingreso"},{code:"4500",name:"Otros Ingresos No Operacionales",type:"Ingreso"},
  {code:"5300",name:"Pérdida en Inversiones",type:"Gasto"},
  {code:"5310",name:"Gastos de Intermediación",type:"Gasto"},{code:"5320",name:"Gastos Legales",type:"Gasto"},
  {code:"5330",name:"Gastos Notariales",type:"Gasto"},{code:"5340",name:"Impuestos y Contribuciones",type:"Gasto"},
  {code:"5350",name:"Intereses Pagados",type:"Gasto"},{code:"5360",name:"Patentes Municipales",type:"Gasto"},
  {code:"5370",name:"Dividendos Pagados",type:"Gasto"},
];
const ACC_TYPES = ["Activo","Pasivo","Patrimonio","Ingreso","Gasto"];

function AccountSelect({value,onChange,accounts}){
  return <select style={S.select} value={value} onChange={e=>onChange(e.target.value)}>
    <option value="">Seleccionar cuenta…</option>
    {ACC_TYPES.map(type=><optgroup key={type} label={`── ${type} ──`}>
      {accounts.filter(a=>a.type===type).sort((a,b)=>a.code.localeCompare(b.code))
        .map(a=><option key={a.code} value={a.code}>{a.code} – {a.name}</option>)}
    </optgroup>)}
  </select>;
}

function AmountInput({value,onChange}){
  const [raw,setRaw]=useState(value===0?"":String(value));
  useEffect(()=>{ if(value===0&&raw!=="") setRaw(""); },[value]);
  return <input style={{...S.input,textAlign:"right"}} type="text" inputMode="numeric" value={raw} placeholder="0"
    onChange={e=>{ const v=e.target.value.replace(/[^0-9]/g,""); setRaw(v); onChange(v===""?0:parseInt(v,10)); }} />;
}

// ── New Entry ──
function NewEntryTab({accounts,entries,setEntries,userId,entityId}){
  const emptyRow=()=>({id:genId(),account:"",debit:0,credit:0,counterparty:""});
  const [date,setDate]=useState(today());
  const [desc,setDesc]=useState(""); const [ref,setRef]=useState("");
  const [rows,setRows]=useState([emptyRow(),emptyRow()]);
  const [err,setErr]=useState(""); const [ok,setOk]=useState("");

  const totD=rows.reduce((s,r)=>s+r.debit,0);
  const totC=rows.reduce((s,r)=>s+r.credit,0);
  const balanced=totD>0&&totD===totC;

  const upd=(id,f,v)=>setRows(rs=>rs.map(r=>r.id===id?{...r,[f]:v}:r));

  async function submit(){
    setErr(""); setOk("");
    if(!date) return setErr("Fecha obligatoria.");
    if(!desc.trim()) return setErr("Descripción obligatoria.");
    if(rows.some(r=>!r.account)) return setErr("Selecciona cuenta en todas las líneas.");
    if(rows.some(r=>r.debit===0&&r.credit===0)) return setErr("Cada línea necesita monto.");
    if(rows.some(r=>r.debit>0&&r.credit>0)) return setErr("Una línea no puede tener débito y crédito.");
    if(!balanced) return setErr(`No cuadra. Diff: ${fmtCLP(Math.abs(totD-totC))}`);
    const n=entries.length+1;
    const newEntry={id:genId(),number:n,date,description:desc.trim(),reference:ref.trim(),rows:[...rows],totalDebit:totD,totalCredit:totC,createdAt:new Date().toISOString()};
    const updated=[...entries, newEntry];
    setEntries(updated);
    await dbUpsertEntry(userId, entityId, newEntry, updated);
    setDesc(""); setRef(""); setDate(today()); setRows([emptyRow(),emptyRow()]);
    setOk(`✓ Asiento N° ${n} registrado.`); setTimeout(()=>setOk(""),4000);
  }

  return <div>
    {ok&&<Msg ok>{ok}</Msg>}{err&&<Msg>{err}</Msg>}
    <div style={S.card}>
      <div style={S.cHead()}><span style={S.cTitle}>Datos del Asiento</span></div>
      <div style={S.cBody}>
        <div style={S.g3}>
          <Inp label="Fecha *" type="date" value={date} onChange={e=>setDate(e.target.value)}/>
          <Inp label="Glosa / Descripción *" value={desc} onChange={e=>setDesc(e.target.value)} placeholder="Ej: Pago arriendo octubre"/>
          <Inp label="N° Documento / Referencia" value={ref} onChange={e=>setRef(e.target.value)} placeholder="Factura 001-234"/>
        </div>
      </div>
    </div>
    <div style={S.card}>
      <div style={S.cHead()}><span style={S.cTitle}>Líneas</span><Btn onClick={()=>setRows(r=>[...r,emptyRow()])} sm v="gold">+ Línea</Btn></div>
      <div style={{overflowX:"auto"}}>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:13}}>
          <thead><tr>
            <th style={{...S.th,width:36}}>#</th>
            <th style={{...S.th,width:"35%"}}>Cuenta</th>
            <th style={{...S.th,width:"18%"}}>Tercero (opcional)</th>
            <th style={{...S.th,textAlign:"right"}}>Débito ($)</th>
            <th style={{...S.th,textAlign:"right"}}>Crédito ($)</th>
            <th style={{...S.th,width:40}}></th>
          </tr></thead>
          <tbody>{rows.map((row,i)=><tr key={row.id} style={{background:i%2===0?"#fafaf9":"#fff"}}>
            <td style={{...S.td,color:C.muted,fontSize:11}}>{i+1}</td>
            <td style={S.td}><AccountSelect value={row.account} onChange={v=>upd(row.id,"account",v)} accounts={accounts}/></td>
            <td style={S.td}><input style={{...S.input,fontSize:12}} placeholder="FPO, WYA…" value={row.counterparty||""} onChange={e=>upd(row.id,"counterparty",e.target.value)}/></td>
            <td style={S.td}><AmountInput value={row.debit} onChange={v=>upd(row.id,"debit",v)}/></td>
            <td style={S.td}><AmountInput value={row.credit} onChange={v=>upd(row.id,"credit",v)}/></td>
            <td style={S.td}>{rows.length>2&&<button style={S.bsm("transparent",C.danger)} onClick={()=>setRows(rs=>rs.filter(r=>r.id!==row.id))}>✕</button>}</td>
          </tr>)}</tbody>
          <tfoot><tr style={{background:C.navy}}>
            <td colSpan={3} style={{...S.td,color:C.gold,fontWeight:700,fontSize:10.5,letterSpacing:2,textTransform:"uppercase"}}>TOTALES</td>
            <td style={{...S.td,textAlign:"right",fontWeight:700,fontSize:15,color:balanced?C.green:"#f87171"}}>{fmtCLP(totD)}</td>
            <td style={{...S.td,textAlign:"right",fontWeight:700,fontSize:15,color:balanced?C.green:"#f87171"}}>{fmtCLP(totC)}</td>
            <td style={S.td}></td>
          </tr></tfoot>
        </table>
      </div>
      <div style={{padding:"14px 22px",borderTop:`1px solid ${C.border}`,display:"flex",alignItems:"center",justifyContent:"space-between"}}>
        <span style={{fontSize:13,color:balanced?C.green:C.danger,fontWeight:700,fontFamily:"'Georgia',serif"}}>
          {balanced?"✅ Asiento cuadrado — listo para registrar":`⚠ Diferencia: ${fmtCLP(Math.abs(totD-totC))}`}
        </span>
        <Btn onClick={submit} disabled={!balanced}>Registrar Asiento →</Btn>
      </div>
    </div>
  </div>;
}

// ── Entries List ──
function EntriesTab({accounts,entries,setEntries,userId,entityId}){
  const [search,setSearch]=useState(""); const [month,setMonth]=useState(""); const [expanded,setExpanded]=useState(null); const [page,setPage]=useState(1);
  const PER=10;
  const accMap=useMemo(()=>Object.fromEntries(accounts.map(a=>[a.code,a])),[accounts]);
  const months=useMemo(()=>[...new Set(entries.map(e=>e.date.slice(0,7)))].sort().reverse(),[entries]);
  const filtered=useMemo(()=>entries.filter(e=>{
    const ms=!search||e.description.toLowerCase().includes(search.toLowerCase())||e.reference?.toLowerCase().includes(search.toLowerCase())||String(e.number).includes(search);
    const mm=!month||e.date.startsWith(month);
    return ms&&mm;
  }).sort((a,b)=>b.date.localeCompare(a.date)||b.number-a.number),[entries,search,month]);
  const pages=Math.ceil(filtered.length/PER);
  const paged=filtered.slice((page-1)*PER,page*PER);

  function del(id){ if(!confirm("¿Eliminar asiento?")) return; const u=entries.filter(e=>e.id!==id); setEntries(u); dbDeleteEntry(userId, id, entityId, u); }
  function exportCSV(){
    const rows=[["N°","Fecha","Descripción","Referencia","Código","Cuenta","Débito","Crédito"]];
    filtered.forEach(e=>e.rows.forEach(r=>{const a=accMap[r.account]||{};rows.push([e.number,e.date,e.description,e.reference||"",r.account,a.name||"",r.debit,r.credit]);}));
    const csv=rows.map(r=>r.map(v=>`"${v}"`).join(",")).join("\n");
    const url=URL.createObjectURL(new Blob(["\uFEFF"+csv],{type:"text/csv;charset=utf-8;"}));
    Object.assign(document.createElement("a"),{href:url,download:"asientos.csv"}).click();
  }
  return <div>
    <div style={S.card}><div style={S.cBody}>
      <div style={{display:"flex",gap:14,flexWrap:"wrap",alignItems:"flex-end"}}>
        <div style={{flex:2,minWidth:180}}><label style={S.label}>Buscar</label><input style={S.input} placeholder="Descripción, referencia, N°…" value={search} onChange={e=>{setSearch(e.target.value);setPage(1);}}/></div>
        <div style={{flex:1,minWidth:150}}><label style={S.label}>Período</label>
          <select style={S.select} value={month} onChange={e=>{setMonth(e.target.value);setPage(1);}}>
            <option value="">Todos</option>
            {months.map(m=><option key={m} value={m}>{new Date(m+"-01T12:00:00").toLocaleDateString("es-CL",{month:"long",year:"numeric"})}</option>)}
          </select>
        </div>
        <Btn v="outline" onClick={exportCSV}>↓ CSV</Btn>
      </div>
    </div></div>
    <div style={{fontSize:11,color:C.muted,marginBottom:10,letterSpacing:1}}>{filtered.length} asiento{filtered.length!==1?"s":""}</div>
    {paged.map(e=>{
      const open=expanded===e.id;
      return <div key={e.id} style={{...S.card,marginBottom:10}}>
        <div style={{padding:"12px 18px",display:"flex",alignItems:"center",justifyContent:"space-between",cursor:"pointer",borderBottom:open?`1px solid ${C.border}`:"none"}} onClick={()=>setExpanded(open?null:e.id)}>
          <div style={{display:"flex",gap:18,alignItems:"center",minWidth:0}}>
            <span style={{fontFamily:"'Georgia',serif",fontWeight:700,color:C.gold,fontSize:14,minWidth:48}}>#{e.number}</span>
            <span style={{color:C.muted,fontSize:11.5,whiteSpace:"nowrap"}}>{fmtDate(e.date)}</span>
            <span style={{fontFamily:"'Georgia',serif",fontSize:13,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{e.description}</span>
            {e.reference&&<span style={{fontSize:11,color:"#aaa",fontStyle:"italic",whiteSpace:"nowrap"}}>{e.reference}</span>}
          </div>
          <div style={{display:"flex",gap:14,alignItems:"center",flexShrink:0}}>
            <span style={{fontFamily:"'Georgia',serif",fontWeight:700}}>{fmtCLP(e.totalDebit)}</span>
            <button style={S.bsm("transparent",C.danger)} onClick={ev=>{ev.stopPropagation();del(e.id);}}>✕</button>
            <span style={{color:C.muted}}>{open?"▲":"▼"}</span>
          </div>
        </div>
        {open&&<div style={{overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:12.5}}>
            <thead><tr>
              {["Código","Cuenta","Tipo","Tercero","Débito","Crédito"].map((h,i)=><th key={i} style={{...S.th,textAlign:i>=4?"right":"left"}}>{h}</th>)}
            </tr></thead>
            <tbody>{e.rows.map((r,i)=>{const a=accMap[r.account]||{};return <tr key={i} style={{background:i%2===0?"#fafaf9":"#fff"}}>
              <td style={{...S.td,fontFamily:"monospace",fontSize:11,color:C.muted}}>{r.account}</td>
              <td style={{...S.td,fontFamily:"'Georgia',serif"}}>{a.name||r.account}</td>
              <td style={S.td}>{a.type&&<span style={S.tag(a.type)}>{a.type}</span>}</td>
              <td style={{...S.td,fontSize:11,color:C.muted}}>{r.counterparty||"—"}</td>
              <td style={{...S.td,textAlign:"right",fontWeight:r.debit>0?700:400,color:r.debit>0?C.navy:C.border}}>{r.debit>0?fmtCLP(r.debit):"—"}</td>
              <td style={{...S.td,textAlign:"right",fontWeight:r.credit>0?700:400,color:r.credit>0?C.navy:C.border}}>{r.credit>0?fmtCLP(r.credit):"—"}</td>
            </tr>;})}</tbody>
            <tfoot><tr style={{background:"#f1f5f9"}}>
              <td colSpan={4} style={{...S.td,fontWeight:700,fontSize:10,letterSpacing:1,textTransform:"uppercase"}}>Total</td>
              <td style={{...S.td,textAlign:"right",fontWeight:700}}>{fmtCLP(e.totalDebit)}</td>
              <td style={{...S.td,textAlign:"right",fontWeight:700}}>{fmtCLP(e.totalCredit)}</td>
            </tr></tfoot>
          </table>
        </div>}
      </div>;
    })}
    {pages>1&&<div style={{display:"flex",gap:8,justifyContent:"center",marginTop:14}}>
      <Btn v="outline" disabled={page===1} onClick={()=>setPage(p=>p-1)}>← Anterior</Btn>
      <span style={{padding:"8px 14px",fontSize:12,color:C.muted}}>Pág {page} / {pages}</span>
      <Btn v="outline" disabled={page===pages} onClick={()=>setPage(p=>p+1)}>Siguiente →</Btn>
    </div>}
  </div>;
}


// ── Reports ──
function ReportsTab({accounts,entries}){
  const [type,setType]=useState("balance");
  const accMap=useMemo(()=>Object.fromEntries(accounts.map(a=>[a.code,a])),[accounts]);

  // All available years and months
  const allYears=useMemo(()=>[...new Set(entries.map(e=>e.date.slice(0,4)))].sort(),[entries]);
  const allMonths=useMemo(()=>[...new Set(entries.map(e=>e.date.slice(0,7)))].sort(),[entries]);

  // Balance: acumulado hasta un mes (o todo)
  const [balCutoff,setBalCutoff]=useState("");         // "" = todos | "YYYY-MM" = hasta ese mes inclusive

  // EERR: rango libre por año o por rango de meses
  const [eerrMode,setEerrMode]=useState("year");       // "year" | "range"
  const [eerrYear,setEerrYear]=useState(()=>allYears[allYears.length-1]||"");
  const [eerrFrom,setEerrFrom]=useState("");
  const [eerrTo,setEerrTo]=useState("");

  // Libro diario: rango libre
  const [ldFrom,setLdFrom]=useState("");
  const [ldTo,setLdTo]=useState("");
  const [selAccCode,setSelAccCode]=useState(null); // selected account for Cuenta T view

  // ── Filtered sets ──
  const balEntries=useMemo(()=>
    balCutoff ? entries.filter(e=>e.date.slice(0,7)<=balCutoff) : entries
  ,[entries,balCutoff]);

  const eerrEntries=useMemo(()=>{
    if(eerrMode==="year" && eerrYear)
      return entries.filter(e=>e.date.startsWith(eerrYear));
    const from=eerrFrom||"0000-01", to=eerrTo||"9999-12";
    return entries.filter(e=>{ const m=e.date.slice(0,7); return m>=from && m<=to; });
  },[entries,eerrMode,eerrYear,eerrFrom,eerrTo]);

  const ldEntries=useMemo(()=>{
    const from=ldFrom||"0000-01", to=ldTo||"9999-12";
    return [...entries.filter(e=>{ const m=e.date.slice(0,7); return m>=from && m<=to; })]
      .sort((a,b)=>a.date.localeCompare(b.date)||a.number-b.number);
  },[entries,ldFrom,ldTo]);

  // ── Balance trial ──
  const balances=useMemo(()=>{
    const b={};
    balEntries.forEach(e=>e.rows.forEach(r=>{
      if(!b[r.account])b[r.account]={debit:0,credit:0};
      b[r.account].debit+=r.debit; b[r.account].credit+=r.credit;
    }));
    return b;
  },[balEntries]);

  // Cuentas conocidas con movimientos
  const trial=useMemo(()=>accounts.filter(a=>balances[a.code]).map(a=>{
    const b=balances[a.code]||{debit:0,credit:0};
    return{...a,...b,saldo:b.debit-b.credit};
  }).sort((a,b_)=>a.code.localeCompare(b_.code)),[accounts,balances]);

  // Cuentas con movimientos pero SIN definir en el plan — no deben perderse del total
  const unmappedCodes=useMemo(()=>{
    const known=new Set(accounts.map(a=>a.code));
    return Object.keys(balances).filter(c=>!known.has(c));
  },[accounts,balances]);
  const unmappedRows=useMemo(()=>unmappedCodes.map(c=>({
    code:c, name:`⚠ Sin mapear (${c})`, type:"?",
    debit:balances[c].debit, credit:balances[c].credit,
    saldo:balances[c].debit-balances[c].credit
  })),[unmappedCodes,balances]);

  // Totales REALES desde balances completos (incluye no mapeadas)
  const totD=useMemo(()=>Object.values(balances).reduce((s,b)=>s+b.debit,0),[balances]);
  const totC=useMemo(()=>Object.values(balances).reduce((s,b)=>s+b.credit,0),[balances]);

  // ── EERR ──
  const eerrBalances=useMemo(()=>{
    const b={};
    eerrEntries.forEach(e=>e.rows.forEach(r=>{
      if(!b[r.account])b[r.account]={debit:0,credit:0};
      b[r.account].debit+=r.debit; b[r.account].credit+=r.credit;
    }));
    return b;
  },[eerrEntries]);

  const eerrTrial=useMemo(()=>accounts.filter(a=>eerrBalances[a.code]).map(a=>{
    const b=eerrBalances[a.code]||{debit:0,credit:0};
    return{...a,...b,saldo:b.debit-b.credit};
  }),[accounts,eerrBalances]);

  const income=eerrTrial.filter(r=>r.type==="Ingreso").sort((a,b)=>a.code.localeCompare(b.code));
  const expenses=eerrTrial.filter(r=>r.type==="Gasto").sort((a,b)=>a.code.localeCompare(b.code));
  const totInc=income.reduce((s,r)=>s+(r.credit-r.debit),0);
  const totExp=expenses.reduce((s,r)=>s+(r.debit-r.credit),0);
  const netResult=totInc-totExp;

  // ── EERR period label ──
  const eerrLabel=useMemo(()=>{
    if(eerrMode==="year") return eerrYear ? `Año ${eerrYear}` : "Todos los períodos";
    const fmtM=m=>m?new Date(m+"-01T12:00:00").toLocaleDateString("es-CL",{month:"short",year:"numeric"}):"—";
    if(eerrFrom&&eerrTo) return `${fmtM(eerrFrom)} — ${fmtM(eerrTo)}`;
    if(eerrFrom) return `Desde ${fmtM(eerrFrom)}`;
    if(eerrTo) return `Hasta ${fmtM(eerrTo)}`;
    return "Todos los períodos";
  },[eerrMode,eerrYear,eerrFrom,eerrTo]);

  // ── Export CSV ──
  function exportCSV(){
    let rows;
    if(type==="ledger"){
      rows=[["N°","Fecha","Descripción","Código","Cuenta","Tipo","Tercero","Débito","Crédito"]];
      ldEntries.forEach(e=>e.rows.forEach(r=>{const a=accMap[r.account]||{};rows.push([e.number,e.date,e.description,r.account,a.name||"",a.type||"",r.counterparty||"",r.debit,r.credit]);}));
    } else if(type==="balance"){
      rows=[["Código","Cuenta","Tipo","Débito Acum.","Crédito Acum.","Saldo"]];
      trial.forEach(r=>rows.push([r.code,r.name,r.type,r.debit,r.credit,r.saldo]));
    } else {
      rows=[["Tipo","Código","Cuenta","Monto"]];
      income.forEach(r=>rows.push(["Ingreso",r.code,r.name,r.credit-r.debit]));
      expenses.forEach(r=>rows.push(["Gasto",r.code,r.name,r.debit-r.credit]));
      rows.push(["Resultado","","",netResult]);
    }
    const csv=rows.map(r=>r.map(v=>`"${v}"`).join(",")).join("\n");
    const url=URL.createObjectURL(new Blob(["\uFEFF"+csv],{type:"text/csv;charset=utf-8;"}));
    Object.assign(document.createElement("a"),{href:url,download:`${type}_${eerrLabel.replace(/[^a-zA-Z0-9]/g,"_")}.csv`}).click();
  }

  const fmtMonthLabel=m=>m?new Date(m+"-01T12:00:00").toLocaleDateString("es-CL",{month:"long",year:"numeric"}):"";

  return <div>
    {/* ── Selector tipo reporte ── */}
    <div style={S.card}><div style={S.cBody}>
      <div style={{display:"flex",gap:14,flexWrap:"wrap",alignItems:"flex-end"}}>
        <div style={{flex:"0 0 220px"}}><label style={S.label}>Tipo de reporte</label>
          <select style={S.select} value={type} onChange={e=>setType(e.target.value)}>
            <option value="balance">Balance de Comprobación</option>
            <option value="results">Estado de Resultados</option>
            <option value="ledger">Libro Diario</option>
            <option value="cuentasT">Cuentas T</option>
          </select>
        </div>

        {/* Balance: acumulado hasta fecha */}
        {type==="balance"&&<div style={{flex:1}}>
          <label style={S.label}>Acumulado hasta</label>
          <select style={S.select} value={balCutoff} onChange={e=>setBalCutoff(e.target.value)}>
            <option value="">Todos los períodos (acumulado total)</option>
            {[...allMonths].reverse().map(m=><option key={m} value={m}>Hasta {fmtMonthLabel(m)}</option>)}
          </select>
          <div style={{fontSize:10,color:C.muted,marginTop:3}}>El balance siempre es acumulado desde el inicio hasta el período seleccionado.</div>
        </div>}

        {/* EERR: año o rango */}
        {type==="results"&&<>
          <div style={{flex:"0 0 160px"}}><label style={S.label}>Modo</label>
            <select style={S.select} value={eerrMode} onChange={e=>setEerrMode(e.target.value)}>
              <option value="year">Por año</option>
              <option value="range">Rango de meses</option>
            </select>
          </div>
          {eerrMode==="year"&&<div style={{flex:"0 0 130px"}}><label style={S.label}>Año</label>
            <select style={S.select} value={eerrYear} onChange={e=>setEerrYear(e.target.value)}>
              <option value="">Todos</option>
              {allYears.map(y=><option key={y} value={y}>{y}</option>)}
            </select>
          </div>}
          {eerrMode==="range"&&<>
            <div style={{flex:"0 0 170px"}}><label style={S.label}>Desde</label>
              <select style={S.select} value={eerrFrom} onChange={e=>setEerrFrom(e.target.value)}>
                <option value="">Inicio</option>
                {allMonths.map(m=><option key={m} value={m}>{fmtMonthLabel(m)}</option>)}
              </select>
            </div>
            <div style={{flex:"0 0 170px"}}><label style={S.label}>Hasta</label>
              <select style={S.select} value={eerrTo} onChange={e=>setEerrTo(e.target.value)}>
                <option value="">Fin</option>
                {[...allMonths].reverse().map(m=><option key={m} value={m}>{fmtMonthLabel(m)}</option>)}
              </select>
            </div>
          </>}
        </>}

        {/* Libro diario: rango */}
        {(type==="ledger"||type==="cuentasT")&&<>
          <div style={{flex:"0 0 170px"}}><label style={S.label}>Desde</label>
            <select style={S.select} value={ldFrom} onChange={e=>setLdFrom(e.target.value)}>
              <option value="">Inicio</option>
              {allMonths.map(m=><option key={m} value={m}>{fmtMonthLabel(m)}</option>)}
            </select>
          </div>
          <div style={{flex:"0 0 170px"}}><label style={S.label}>Hasta</label>
            <select style={S.select} value={ldTo} onChange={e=>setLdTo(e.target.value)}>
              <option value="">Fin</option>
              {[...allMonths].reverse().map(m=><option key={m} value={m}>{fmtMonthLabel(m)}</option>)}
            </select>
          </div>
        </>}

        <Btn v="outline" onClick={exportCSV}>↓ CSV</Btn>
      </div>
    </div></div>

    {/* ── Balance de Comprobación (siempre acumulado) ── */}
    {type==="balance"&&<div style={S.card}>
      <div style={S.cHead()}>
        <span style={S.cTitle}>Balance de Comprobación{balCutoff?` — acumulado hasta ${fmtMonthLabel(balCutoff)}`:""}</span>
        <span style={{background:Math.abs(totD-totC)<1?"#dcfce7":"#fef2f2",color:Math.abs(totD-totC)<1?C.green:C.danger,padding:"3px 10px",borderRadius:10,fontSize:10,fontWeight:700}}>{Math.abs(totD-totC)<1?"✓ Balanceado":"⚠ Desbalanceado"}</span>
      </div>
      {trial.length===0&&unmappedRows.length===0?<div style={S.empty}><div style={{fontFamily:"'Georgia',serif",color:C.muted}}>Sin movimientos en el período</div></div>
      :<div style={{overflowX:"auto"}}>
        {unmappedRows.length>0&&<div style={{background:"#fef9c3",border:"1px solid #f59e0b",borderRadius:3,padding:"8px 14px",margin:"0 0 8px",fontSize:12,color:"#92400e"}}>
          ⚠ Hay {unmappedRows.length} cuenta(s) con movimientos no definidas en el plan de cuentas. Agrégalas en <b>Plan de Cuentas</b> para clasificarlas correctamente.
          {unmappedRows.map(r=><span key={r.code} style={{marginLeft:8,fontFamily:"monospace",fontWeight:700}}>{r.code}</span>)}
        </div>}
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:12.5}}>
        <thead><tr>{["Código","Cuenta","Tipo","Débito Acum.","Crédito Acum.","Saldo"].map((h,i)=><th key={i} style={{...S.th,textAlign:i>=3?"right":"left"}}>{h}</th>)}</tr></thead>
        <tbody>
          {trial.map((r,i)=><tr key={r.code} style={{background:i%2===0?"#fafaf9":"#fff"}}>
            <td style={{...S.td,fontFamily:"monospace",fontSize:11,color:C.muted}}>{r.code}</td>
            <td style={{...S.td,fontFamily:"'Georgia',serif"}}>{r.name}</td>
            <td style={S.td}><span style={S.tag(r.type)}>{r.type}</span></td>
            <td style={{...S.td,textAlign:"right"}}>{r.debit>0?fmtCLP(r.debit):"—"}</td>
            <td style={{...S.td,textAlign:"right"}}>{r.credit>0?fmtCLP(r.credit):"—"}</td>
            <td style={{...S.td,textAlign:"right",fontWeight:700,color:r.saldo>=0?C.navy:C.danger}}>{r.saldo>=0?fmtCLP(r.saldo):`(${fmtCLP(Math.abs(r.saldo))})`}</td>
          </tr>)}
          {unmappedRows.map((r,i)=><tr key={r.code} style={{background:"#fef9c3"}}>
            <td style={{...S.td,fontFamily:"monospace",fontSize:11,color:"#92400e",fontWeight:700}}>{r.code}</td>
            <td style={{...S.td,color:"#92400e"}}>{r.name}</td>
            <td style={{...S.td,color:"#92400e",fontSize:11}}>Sin clasificar</td>
            <td style={{...S.td,textAlign:"right",color:"#92400e"}}>{r.debit>0?fmtCLP(r.debit):"—"}</td>
            <td style={{...S.td,textAlign:"right",color:"#92400e"}}>{r.credit>0?fmtCLP(r.credit):"—"}</td>
            <td style={{...S.td,textAlign:"right",fontWeight:700,color:"#92400e"}}>{r.saldo>=0?fmtCLP(r.saldo):`(${fmtCLP(Math.abs(r.saldo))})`}</td>
          </tr>)}
        </tbody>
        <tfoot><tr style={{background:C.navy}}>
          <td colSpan={3} style={{...S.td,color:C.gold,fontWeight:700,fontSize:10,letterSpacing:2,textTransform:"uppercase"}}>Total</td>
          <td style={{...S.td,textAlign:"right",color:"#fff",fontWeight:700}}>{fmtCLP(totD)}</td>
          <td style={{...S.td,textAlign:"right",color:"#fff",fontWeight:700}}>{fmtCLP(totC)}</td>
          <td style={{...S.td,textAlign:"right",color:Math.abs(totD-totC)<1?C.gold:"#f87171",fontWeight:700}}>{fmtCLP(Math.abs(totD-totC))}</td>
        </tr></tfoot>
      </table></div>}
    </div>}

    {/* ── Estado de Resultados ── */}
    {type==="results"&&<div style={S.card}>
      <div style={S.cHead()}>
        <span style={S.cTitle}>Estado de Resultados — {eerrLabel}</span>
        <span style={{fontSize:11,color:C.gold}}>{eerrEntries.length} asientos</span>
      </div>
      <div style={S.cBody}><div style={{maxWidth:660}}>
        {income.length===0&&expenses.length===0&&<div style={{...S.empty,padding:20}}><div style={{color:C.muted}}>Sin movimientos de resultado en el período seleccionado.</div></div>}

        {/* Ingresos — contribución positiva */}
        {income.length>0&&<>
          <div style={{fontSize:10,fontWeight:700,letterSpacing:2,textTransform:"uppercase",color:C.muted,marginBottom:8,paddingBottom:6,borderBottom:`1px solid ${C.border}`}}>Ingresos</div>
          {income.map(r=>{
            const monto=r.credit-r.debit; // positivo = ingreso neto
            return <div key={r.code} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"7px 0",borderBottom:`1px solid ${C.border}`}}>
              <span style={{fontFamily:"'Georgia',serif",display:"flex",gap:8,alignItems:"center"}}>
                <code style={{fontSize:10,color:C.muted,minWidth:36}}>{r.code}</code>{r.name}
              </span>
              <span style={{fontWeight:700,color:monto>=0?C.green:C.danger,minWidth:130,textAlign:"right"}}>
                {monto>=0?"+":""}{fmtCLP(monto)}
              </span>
            </div>;
          })}
          <div style={{display:"flex",justifyContent:"space-between",padding:"10px 0",borderBottom:`2px solid ${C.navy}`,marginTop:4,marginBottom:18}}>
            <span style={{fontWeight:700,fontSize:11,letterSpacing:1,textTransform:"uppercase"}}>Total Ingresos</span>
            <span style={{fontWeight:700,fontSize:15,color:totInc>=0?C.green:C.danger,minWidth:130,textAlign:"right"}}>
              {totInc>=0?"+":""}{fmtCLP(totInc)}
            </span>
          </div>
        </>}

        {/* Gastos — contribución negativa al resultado */}
        {expenses.length>0&&<>
          <div style={{fontSize:10,fontWeight:700,letterSpacing:2,textTransform:"uppercase",color:C.muted,marginBottom:8,paddingBottom:6,borderBottom:`1px solid ${C.border}`}}>Gastos</div>
          {expenses.map(r=>{
            const monto=-(r.debit-r.credit); // negativo = reduce resultado
            return <div key={r.code} style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"7px 0",borderBottom:`1px solid ${C.border}`}}>
              <span style={{fontFamily:"'Georgia',serif",display:"flex",gap:8,alignItems:"center"}}>
                <code style={{fontSize:10,color:C.muted,minWidth:36}}>{r.code}</code>{r.name}
              </span>
              <span style={{fontWeight:700,color:monto<0?C.danger:C.green,minWidth:130,textAlign:"right"}}>
                {monto>=0?"+":""}{fmtCLP(monto)}
              </span>
            </div>;
          })}
          <div style={{display:"flex",justifyContent:"space-between",padding:"10px 0",borderBottom:`2px solid ${C.navy}`,marginTop:4,marginBottom:18}}>
            <span style={{fontWeight:700,fontSize:11,letterSpacing:1,textTransform:"uppercase"}}>Total Gastos</span>
            <span style={{fontWeight:700,fontSize:15,color:C.danger,minWidth:130,textAlign:"right"}}>
              -{fmtCLP(totExp)}
            </span>
          </div>
        </>}

        {/* Resultado neto */}
        {(income.length>0||expenses.length>0)&&<div style={{display:"flex",justifyContent:"space-between",alignItems:"center",padding:"18px 24px",background:netResult>=0?C.greenBg:C.redBg,border:`1px solid ${netResult>=0?C.green:C.danger}`,borderRadius:4}}>
          <div>
            <div style={{fontWeight:700,fontSize:13,fontFamily:"'Georgia',serif"}}>Resultado del Ejercicio</div>
            <div style={{fontSize:10,color:C.muted,marginTop:2}}>{eerrLabel}</div>
          </div>
          <span style={{fontWeight:700,fontSize:26,color:netResult>=0?C.green:C.danger}}>
            {netResult>=0?"+":""}{fmtCLP(netResult)}
          </span>
        </div>}
      </div></div>
    </div>}

    {/* ── Cuentas T ── */}
    {type==="cuentasT"&&(()=>{
      // Build per-account movements from ldEntries
      const tData={};
      ldEntries.forEach(e=>{
        e.rows.forEach(r=>{
          if(!tData[r.account]) tData[r.account]={debit:[],credit:[]};
          const line={date:e.date,desc:e.description,amount:r.debit||r.credit,counterparty:r.counterparty||""};
          if(r.debit>0) tData[r.account].debit.push(line);
          else if(r.credit>0) tData[r.account].credit.push(line);
        });
      });
      const sortedAccs=Object.keys(tData).sort();
      if(sortedAccs.length===0) return <div style={{...S.card,...S.empty}}><div style={{color:C.muted}}>Sin movimientos en el período</div></div>;

      // Ensure selAccCode is valid, default to first
      const activeCode = (selAccCode && tData[selAccCode]) ? selAccCode : sortedAccs[0];

      // Render account T detail
      function renderT(code){
        const acc=accMap[code]||{name:code,type:"?"};
        const {debit,credit}=tData[code];
        const totD=debit.reduce((s,r)=>s+r.amount,0);
        const totC=credit.reduce((s,r)=>s+r.amount,0);
        const saldo=totD-totC;
        return <div style={S.card}>
          <div style={{...S.cHead(),borderRadius:"4px 4px 0 0"}}>
            <div style={{display:"flex",alignItems:"center",gap:12}}>
              <code style={{fontSize:13,color:C.gold,fontWeight:700}}>{code}</code>
              <span style={{...S.cTitle,fontSize:13}}>{acc.name}</span>
              {acc.type&&acc.type!=="?"&&<span style={S.tag(acc.type)}>{acc.type}</span>}
            </div>
            <div style={{display:"flex",gap:20,fontSize:12}}>
              <span style={{color:"#94a3b8"}}>Débitos: <b style={{color:"#fff"}}>{fmtCLP(totD)}</b></span>
              <span style={{color:"#94a3b8"}}>Créditos: <b style={{color:"#fff"}}>{fmtCLP(totC)}</b></span>
              <span style={{color:C.gold}}>Saldo: <b style={{color:saldo>=0?"#86efac":"#fca5a5"}}>{saldo>=0?"+":""}{fmtCLP(saldo)}</b></span>
            </div>
          </div>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",borderBottom:`1px solid ${C.border}`}}>
            <div style={{background:"#f0fdf4",padding:"7px 14px",borderRight:`1px solid ${C.border}`,borderBottom:`1px solid ${C.border}`}}>
              <span style={{fontSize:10,fontWeight:700,letterSpacing:2,textTransform:"uppercase",color:C.green}}>DEBE</span>
            </div>
            <div style={{background:"#fef2f2",padding:"7px 14px",borderBottom:`1px solid ${C.border}`}}>
              <span style={{fontSize:10,fontWeight:700,letterSpacing:2,textTransform:"uppercase",color:C.danger}}>HABER</span>
            </div>
          </div>
          <div style={{display:"grid",gridTemplateColumns:"1fr 1fr"}}>
            <div style={{borderRight:`1px solid ${C.border}`}}>
              {debit.length===0
                ? <div style={{padding:"10px 14px",fontSize:12,color:C.muted,fontStyle:"italic"}}>Sin movimientos</div>
                : debit.map((r,i)=><div key={i} style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",padding:"7px 14px",borderBottom:`1px solid ${C.border}`,background:i%2===0?"#fafaf9":"#fff",gap:8}}>
                    <div style={{flex:1,minWidth:0}}>
                      <div style={{fontSize:11.5,color:C.muted,fontFamily:"monospace",whiteSpace:"nowrap"}}>{fmtDate(r.date)}</div>
                      <div style={{fontSize:12,color:C.navy,fontFamily:"'Georgia',serif",marginTop:1,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",maxWidth:220}} title={r.desc}>{r.desc}</div>
                      {r.counterparty&&<div style={{fontSize:10.5,color:C.muted,marginTop:1}}>{r.counterparty}</div>}
                    </div>
                    <div style={{fontWeight:700,fontSize:13,color:C.green,whiteSpace:"nowrap"}}>{fmtCLP(r.amount)}</div>
                  </div>)
              }
              {debit.length>0&&<div style={{display:"flex",justifyContent:"flex-end",padding:"8px 14px",background:"#f0fdf4",borderTop:`2px solid ${C.navy}`}}>
                <span style={{fontWeight:700,fontSize:13,color:C.green}}>{fmtCLP(totD)}</span>
              </div>}
            </div>
            <div>
              {credit.length===0
                ? <div style={{padding:"10px 14px",fontSize:12,color:C.muted,fontStyle:"italic"}}>Sin movimientos</div>
                : credit.map((r,i)=><div key={i} style={{display:"flex",justifyContent:"space-between",alignItems:"flex-start",padding:"7px 14px",borderBottom:`1px solid ${C.border}`,background:i%2===0?"#fafaf9":"#fff",gap:8}}>
                    <div style={{flex:1,minWidth:0}}>
                      <div style={{fontSize:11.5,color:C.muted,fontFamily:"monospace",whiteSpace:"nowrap"}}>{fmtDate(r.date)}</div>
                      <div style={{fontSize:12,color:C.navy,fontFamily:"'Georgia',serif",marginTop:1,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",maxWidth:220}} title={r.desc}>{r.desc}</div>
                      {r.counterparty&&<div style={{fontSize:10.5,color:C.muted,marginTop:1}}>{r.counterparty}</div>}
                    </div>
                    <div style={{fontWeight:700,fontSize:13,color:C.danger,whiteSpace:"nowrap"}}>{fmtCLP(r.amount)}</div>
                  </div>)
              }
              {credit.length>0&&<div style={{display:"flex",justifyContent:"flex-end",padding:"8px 14px",background:"#fef2f2",borderTop:`2px solid ${C.navy}`}}>
                <span style={{fontWeight:700,fontSize:13,color:C.danger}}>{fmtCLP(totC)}</span>
              </div>}
            </div>
          </div>
          <div style={{display:"flex",justifyContent:"flex-end",alignItems:"center",gap:8,padding:"8px 16px",background:"#f8fafc",borderTop:`1px solid ${C.border}`}}>
            <span style={{fontSize:10,letterSpacing:1,textTransform:"uppercase",color:C.muted}}>Saldo</span>
            <span style={{fontWeight:700,fontSize:14,color:saldo>=0?C.green:C.danger}}>{saldo>=0?"+":""}{fmtCLP(saldo)}</span>
          </div>
        </div>;
      }

      return <div style={{display:"grid",gridTemplateColumns:"220px 1fr",gap:16,alignItems:"start"}}>
        {/* Left: account list */}
        <div style={S.card}>
          <div style={{...S.cHead(),padding:"10px 14px"}}><span style={{...S.cTitle,fontSize:11}}>CUENTAS ({sortedAccs.length})</span></div>
          <div style={{overflowY:"auto",maxHeight:600}}>
            {sortedAccs.map(code=>{
              const acc=accMap[code]||{name:code,type:"?"};
              const d=tData[code];
              const totD=d.debit.reduce((s,r)=>s+r.amount,0);
              const totC=d.credit.reduce((s,r)=>s+r.amount,0);
              const saldo=totD-totC;
              const isActive=code===activeCode;
              return <div key={code}
                onClick={()=>setSelAccCode(code)}
                style={{padding:"9px 14px",borderBottom:`1px solid ${C.border}`,cursor:"pointer",
                  background:isActive?C.navy:"#fff",
                  borderLeft:isActive?`3px solid ${C.gold}`:"3px solid transparent",
                  transition:"background 0.1s"}}>
                <div style={{display:"flex",justifyContent:"space-between",alignItems:"center",gap:6}}>
                  <div style={{minWidth:0}}>
                    <code style={{fontSize:10,color:isActive?C.gold:C.muted,display:"block"}}>{code}</code>
                    <div style={{fontSize:12,fontWeight:isActive?700:400,color:isActive?"#fff":C.navy,
                      overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",maxWidth:130,
                      fontFamily:"'Georgia',serif"}}>{acc.name}</div>
                  </div>
                  <div style={{textAlign:"right",whiteSpace:"nowrap"}}>
                    <div style={{fontSize:10,fontWeight:700,color:saldo>=0?(isActive?"#86efac":C.green):(isActive?"#fca5a5":C.danger)}}>
                      {saldo>=0?"+":""}{fmtCLP(Math.abs(saldo))}
                    </div>
                    <div style={{fontSize:9,color:isActive?"#94a3b8":C.muted}}>
                      {d.debit.length+d.credit.length} mov
                    </div>
                  </div>
                </div>
              </div>;
            })}
          </div>
        </div>
        {/* Right: selected account T */}
        <div>{renderT(activeCode)}</div>
      </div>;
    })()}

    {/* ── Libro Diario ── */}
    {type==="ledger"&&<div style={S.card}>
      <div style={S.cHead()}><span style={S.cTitle}>Libro Diario ({ldEntries.length} asientos)</span></div>
      {ldEntries.length===0?<div style={S.empty}><div style={{fontFamily:"'Georgia',serif",color:C.muted}}>Sin asientos en el período</div></div>
      :<div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
        <thead><tr>{["N°","Fecha","Glosa","Cuenta","Tercero","Débito","Crédito"].map((h,i)=><th key={i} style={{...S.th,textAlign:i>=5?"right":"left"}}>{h}</th>)}</tr></thead>
        <tbody>{ldEntries.map(e=>e.rows.map((r,ri)=>{const a=accMap[r.account]||{}; return <tr key={r.id||ri} style={{background:ri%2===0?"#fafaf9":"#fff"}}>
          {ri===0&&<td style={{...S.td,fontWeight:700,color:C.gold,fontFamily:"'Georgia',serif",whiteSpace:"nowrap"}} rowSpan={e.rows.length}>#{e.number}</td>}
          {ri===0&&<td style={{...S.td,color:C.muted,fontSize:11,whiteSpace:"nowrap"}} rowSpan={e.rows.length}>{fmtDate(e.date)}</td>}
          {ri===0&&<td style={{...S.td,fontFamily:"'Georgia',serif",maxWidth:200}} rowSpan={e.rows.length}>{e.description}</td>}
          <td style={{...S.td,paddingLeft:r.debit===0?24:12}}>{a.name||r.account}</td>
          <td style={{...S.td,fontSize:11,color:C.muted}}>{r.counterparty||"—"}</td>
          <td style={{...S.td,textAlign:"right",fontWeight:r.debit>0?700:400,color:r.debit>0?C.navy:C.border}}>{r.debit>0?fmtCLP(r.debit):"—"}</td>
          <td style={{...S.td,textAlign:"right",fontWeight:r.credit>0?700:400,color:r.credit>0?C.navy:C.border}}>{r.credit>0?fmtCLP(r.credit):"—"}</td>
        </tr>;}))}
        </tbody>
      </table></div>}
    </div>}
  </div>;
}

// ── Accounts (Plan de Cuentas) ──
function AccountsTab({accounts,setAccounts,userId,entityId}){
  const [f,setF]=useState({code:"",name:"",type:"Activo"});
  const [search,setSearch]=useState(""); const [ft,setFt]=useState(""); const [err,setErr]=useState("");
  const filtered=useMemo(()=>accounts.filter(a=>(!search||a.code.includes(search)||a.name.toLowerCase().includes(search.toLowerCase()))&&(!ft||a.type===ft)).sort((a,b)=>a.code.localeCompare(b.code)),[accounts,search,ft]);
  function add(){ setErr(""); if(!f.code.trim()||!f.name.trim()) return setErr("Código y nombre obligatorios."); if(accounts.some(a=>a.code===f.code.trim())) return setErr("Código ya existe."); const newAcc={code:f.code.trim(),name:f.name.trim(),type:f.type}; const u=[...accounts,newAcc]; setAccounts(u); dbUpsertAccount(userId,newAcc,u); setF({code:"",name:"",type:"Activo"}); }
  function del(code){ if(!confirm("¿Eliminar cuenta?")) return; const u=accounts.filter(a=>a.code!==code); setAccounts(u); dbDeleteAccount(userId,code,u); }
  function reset(){ if(!confirm("¿Restaurar plan predeterminado?")) return; setAccounts(DEFAULT_ACCOUNTS); DEFAULT_ACCOUNTS.forEach(a=>dbUpsertAccount(userId,a,DEFAULT_ACCOUNTS)); }
  return <div>
    <div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>Agregar Cuenta</span></div><div style={S.cBody}>
      {err&&<Msg>{err}</Msg>}
      <div style={{display:"flex",gap:14,flexWrap:"wrap",alignItems:"flex-end"}}>
        <div style={{width:120}}><Inp label="Código *" placeholder="1110" value={f.code} onChange={e=>setF(p=>({...p,code:e.target.value}))}/></div>
        <div style={{flex:2,minWidth:180}}><Inp label="Nombre *" placeholder="Banco BCI Corriente" value={f.name} onChange={e=>setF(p=>({...p,name:e.target.value}))}/></div>
        <div style={{width:160}}><Sel label="Tipo" options={ACC_TYPES} value={f.type} onChange={e=>setF(p=>({...p,type:e.target.value}))}/></div>
        <Btn onClick={add}>+ Agregar</Btn>
      </div>
    </div></div>
    <div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>Plan de Cuentas ({accounts.length})</span><Btn v="outline" onClick={reset} sm>↩ Restaurar</Btn></div>
      <div style={{...S.cBody,borderBottom:`1px solid ${C.border}`}}>
        <div style={{display:"flex",gap:14}}>
          <div style={{flex:2}}><input style={S.input} placeholder="Buscar…" value={search} onChange={e=>setSearch(e.target.value)}/></div>
          <div style={{flex:1}}><select style={S.select} value={ft} onChange={e=>setFt(e.target.value)}><option value="">Todos los tipos</option>{ACC_TYPES.map(t=><option key={t}>{t}</option>)}</select></div>
        </div>
      </div>
      <DataTable cols={[
        {label:"Código",fn:a=><code style={{fontSize:11,background:"#f1f5f9",padding:"2px 6px",borderRadius:3}}>{a.code}</code>},
        {label:"Nombre",fn:a=><span style={{fontFamily:"'Georgia',serif"}}>{a.name}</span>},
        {label:"Tipo",fn:a=><span style={S.tag(a.type)}>{a.type}</span>},
        {label:"",fn:a=><button style={S.bsm("transparent",C.danger)} onClick={()=>del(a.code)}>✕</button>},
      ]} rows={filtered}/>
    </div>
  </div>;
}


// ════════════════════════════════════════════════════════════════
//  AMORTIZATION BUILDERS
// ════════════════════════════════════════════════════════════════
function buildFrench(p,r,m,s){ const mr=r/100/12; const c=mr===0?p/m:p*mr*Math.pow(1+mr,m)/(Math.pow(1+mr,m)-1); let bal=p; return Array.from({length:m},(_,i)=>{const int=bal*mr,cap=c-int; bal=Math.max(0,bal-cap); return{period:i+1,date:addMonths(s,i+1),capital:Math.round(cap*100)/100,interest:Math.round(int*100)/100,cuota:Math.round(c*100)/100,balance:Math.round(bal*100)/100,paid:false};}); }
function buildGerman(p,r,m,s){ const mr=r/100/12,cap=p/m; let bal=p; return Array.from({length:m},(_,i)=>{const int=bal*mr,c=cap+int; bal=Math.max(0,bal-cap); return{period:i+1,date:addMonths(s,i+1),capital:Math.round(cap*100)/100,interest:Math.round(int*100)/100,cuota:Math.round(c*100)/100,balance:Math.round(bal*100)/100,paid:false};}); }
function buildBullet(p,r,m,s){ const mr=r/100/12; return Array.from({length:m},(_,i)=>{const last=i===m-1,int=p*mr,cap=last?p:0; return{period:i+1,date:addMonths(s,i+1),capital:Math.round(cap*100)/100,interest:Math.round(int*100)/100,cuota:Math.round((int+cap)*100)/100,balance:last?0:Math.round(p*100)/100,paid:false};}); }
function buildCustomScaffold(m,s){ return Array.from({length:m||1},(_,i)=>({period:i+1,date:addMonths(s,i+1),capital:0,interest:0,cuota:0,balance:0,paid:false})); }
function buildTable(sys,p,r,m,s){ if(sys==="frances") return buildFrench(p,r,m,s); if(sys==="aleman") return buildGerman(p,r,m,s); if(sys==="bullet") return buildBullet(p,r,m,s); return buildCustomScaffold(m,s); }

const AMORT_SYSTEMS=[{v:"frances",l:"Francés (cuota fija)"},{v:"aleman",l:"Alemán (capital fijo)"},{v:"bullet",l:"Bullet (capital al final)"},{v:"personalizado",l:"Personalizado (cuotas manuales)"}];
const CURRENCIES=["CLP","USD","UF","EUR"];

// ── Inline editable cell for custom table ──
function EditCell({value,onChange}){
  const [ed,setEd]=useState(false); const [raw,setRaw]=useState("");
  if(ed) return <input autoFocus style={{...S.input,width:88,padding:"3px 6px",fontSize:11.5,textAlign:"right"}} value={raw}
    onChange={e=>setRaw(e.target.value)}
    onBlur={()=>{onChange(parseFloat(raw)||0);setEd(false);}}
    onKeyDown={e=>{if(e.key==="Enter"||e.key==="Tab"){onChange(parseFloat(raw)||0);setEd(false);}}}/>;
  return <span style={{cursor:"pointer",display:"block",textAlign:"right",padding:"3px 6px",borderRadius:3,background:"#f8f6f1",border:`1px dashed ${C.border}`,fontSize:11.5,minWidth:80}} onClick={()=>{setRaw(String(value||""));setEd(true);}}>
    {value>0?fmtNum(value,2):<span style={{color:"#cbd5e1"}}>0,00</span>}
  </span>;
}

// ════════════════════════════════════════════════════════════════
//  LIABILITIES TAB
// ════════════════════════════════════════════════════════════════
function LiabilityForm({onSave,onCancel,initial,entries}){
  const empty={name:"",lender:"",currency:"CLP",originalAmount:"",annualRate:"",months:"",startDate:today(),system:"frances",notes:"",tags:"",accountingCode:"2400",bankAccount:"1111"};
  const [f,setF]=useState(initial||empty);
  const [customRows,setCustomRows]=useState(initial?.system==="personalizado"?(initial.amortTable||[]):[]);
  const [err,setErr]=useState("");
  const upd=(k,v)=>setF(p=>({...p,[k]:v}));

  // ── Refi from ledger balances ──
  const [refiEnabled,setRefiEnabled]=useState(false);
  const [refiCounterparty,setRefiCounterparty]=useState(""); // filter by counterparty
  const [refiCashDiff,setRefiCashDiff]=useState("");   // +/- cash difference
  const [refiCashAcc,setRefiCashAcc]=useState("1111");
  const [refiDate,setRefiDate]=useState(f.startDate||today());

  // Passive account codes that may carry loan balances
  const PASIVO_ACCS=["2400","2440","2410","2420","2430","2450","2100","2110"];

  // Compute net balance per account+counterparty for ALL passive accounts
  const ledgerBalances=useMemo(()=>{
    if(!entries||entries.length===0) return {};
    const b={};
    entries.forEach(e=>e.rows.forEach(r=>{
      if(!r.account||!PASIVO_ACCS.includes(r.account)) return;
      const cp=(r.counterparty||"").trim();
      const key=r.account+"||"+cp;
      if(!b[key]) b[key]={account:r.account,counterparty:cp,debit:0,credit:0};
      b[key].debit+=r.debit||0;
      b[key].credit+=r.credit||0;
    }));
    return Object.fromEntries(
      Object.entries(b)
        .map(([k,v])=>([k,{...v,balance:v.credit-v.debit}]))
        .filter(([,v])=>v.balance>0.5)
    );
  },[entries]);

  // All counterparties that have any passive balance
  const availableCounterparties=useMemo(()=>{
    const set=new Set();
    Object.values(ledgerBalances).forEach(v=>{ if(v.counterparty) set.add(v.counterparty); });
    return [...set].sort();
  },[ledgerBalances]);

  // All account lines for the selected counterparty (one line per account with balance)
  const refiLines=useMemo(()=>{
    if(!refiEnabled||!refiCounterparty) return [];
    return Object.values(ledgerBalances)
      .filter(v=>v.counterparty===refiCounterparty && v.balance>0.5)
      .sort((a,b)=>a.account.localeCompare(b.account));
  },[ledgerBalances,refiCounterparty,refiEnabled]);

  // Total balance across all accounts for this counterparty
  const selectedBalance=useMemo(()=>refiLines.reduce((s,v)=>s+v.balance,0),[refiLines]);

  const refiCash   = refiEnabled ? (parseFloat(refiCashDiff)||0) : 0;
  const newLoanAmt = refiEnabled ? (selectedBalance + refiCash) : 0;

  function handleSysChange(sys){ upd("system",sys); if(sys==="personalizado"){ const m=parseInt(f.months)||0; if(m>0) setCustomRows(buildCustomScaffold(m,f.startDate||today())); }}
  function handleMonthsChange(val){ upd("months",val); if(f.system==="personalizado"){ const m=parseInt(val)||0; if(m>0) setCustomRows(prev=>Array.from({length:m},(_,i)=>prev[i]||{period:i+1,date:addMonths(f.startDate||today(),i+1),capital:0,interest:0,cuota:0,balance:0,paid:false})); }}
  function updRow(idx,field,value){ setCustomRows(rs=>rs.map((r,i)=>{ if(i!==idx) return r; const u={...r,[field]:value}; if(field==="capital"||field==="interest") u.cuota=Math.round((u.capital+u.interest)*100)/100; return u; })); }

  const preview=useMemo(()=>{
    if(f.system==="personalizado") return null;
    const a=parseFloat(f.originalAmount),r=parseFloat(f.annualRate),m=parseInt(f.months);
    if(!a||r<0||!m||!f.startDate) return null;
    try{return buildTable(f.system,a,r||0,m,f.startDate);}catch{return null;}
  },[f.originalAmount,f.annualRate,f.months,f.startDate,f.system]);

  const customSum=useMemo(()=>f.system!=="personalizado"?null:{
    totalCapital:customRows.reduce((s,r)=>s+r.capital,0),
    totalInterest:customRows.reduce((s,r)=>s+r.interest,0),
    totalCuota:customRows.reduce((s,r)=>s+r.cuota,0),
  },[customRows,f.system]);

  async function submit(){
    setErr("");
    if(!f.name.trim()) return setErr("Nombre obligatorio.");
    const amt=parseFloat(f.originalAmount); if(!amt||amt<=0) return setErr("Monto inválido.");
    if(!f.startDate) return setErr("Fecha inicio obligatoria.");
    let table;
    if(f.system==="personalizado"){ if(customRows.length===0) return setErr("Agrega al menos una cuota."); table=customRows; }
    else{ const rate=parseFloat(f.annualRate); if(isNaN(rate)||rate<0) return setErr("Tasa inválida."); const mo=parseInt(f.months); if(!mo||mo<1) return setErr("Plazo inválido."); table=buildTable(f.system,amt,rate,mo,f.startDate); }
    const mo=f.system==="personalizado"?customRows.length:parseInt(f.months);
    // Build refi metadata if enabled
    const refiMeta = refiEnabled ? {
      refiEnabled:true,
      refiLines,           // [{account, counterparty, balance}, ...]
      refiCounterparty, refiCash, refiCashAcc, refiDate,
      selectedBalance, newLoanAmt:selectedBalance+refiCash,
    } : {refiEnabled:false};

    onSave({id:initial?.id||genId(),...f,originalAmount:amt,annualRate:f.system==="personalizado"?0:(parseFloat(f.annualRate)||0),months:mo,
      amortTable:initial?.amortTable&&f.system!=="personalizado"?initial.amortTable.map((r,i)=>({...(table[i]||table[table.length-1]),paid:r.paid})):table,
      createdAt:initial?.createdAt||new Date().toISOString(), refiMeta});
  }

  const isCustom=f.system==="personalizado";
  return <div style={S.card}>
    <div style={S.cHead()}><span style={S.cTitle}>{initial?"Editar Pasivo":"Nuevo Pasivo"}</span></div>
    <div style={S.cBody}>
      {err&&<Msg>{err}</Msg>}
      <div style={{...S.g3,marginBottom:16}}><Inp label="Nombre *" placeholder="Crédito Banco BCI" value={f.name} onChange={e=>upd("name",e.target.value)}/><Inp label="Acreedor" placeholder="Banco BCI" value={f.lender} onChange={e=>upd("lender",e.target.value)}/><Sel label="Moneda *" options={CURRENCIES} value={f.currency} onChange={e=>upd("currency",e.target.value)}/></div>
      <div style={{...S.g4,marginBottom:16}}>
        <Inp label="Monto original *" type="number" min="0" value={f.originalAmount} onChange={e=>upd("originalAmount",e.target.value)}/>
        {!isCustom&&<Inp label="Tasa anual %" type="number" min="0" step="0.01" placeholder="12.00" value={f.annualRate} onChange={e=>upd("annualRate",e.target.value)}/>}
        <Inp label={isCustom?"N° cuotas (ref.)":"Plazo (meses) *"} type="number" min="1" value={f.months} onChange={e=>handleMonthsChange(e.target.value)}/>
        <Inp label="Fecha inicio *" type="date" value={f.startDate} onChange={e=>upd("startDate",e.target.value)}/>
      </div>
      <div style={{...S.g2,marginBottom:16}}><Sel label="Sistema de amortización" options={AMORT_SYSTEMS} value={f.system} onChange={e=>handleSysChange(e.target.value)}/><Inp label="Etiquetas" placeholder="largo plazo, hipotecario" value={f.tags} onChange={e=>upd("tags",e.target.value)}/></div>
      <div style={{marginBottom:16}}>
        <label style={S.label}>Cuenta contable del pasivo (para asientos automáticos al pagar)</label>
        <select style={S.select} value={f.accountingCode||"2400"} onChange={e=>upd("accountingCode",e.target.value)}>
          <option value="2100">2100 – Cuentas por Pagar Proveedores</option>
          <option value="2110">2110 – Documentos por Pagar</option>
          <option value="2400">2400 – Préstamo Bancario LP</option>
        </select>
        <div style={{fontSize:10.5,color:C.muted,marginTop:4}}>Al marcar una cuota como pagada se generarán asientos automáticos usando esta cuenta.</div>
      </div>
      <div style={{marginBottom:16}}>
        <label style={S.label}>Cuenta bancaria de pago (para asientos automáticos al pagar cuota)</label>
        <select style={S.select} value={f.bankAccount||"1111"} onChange={e=>upd("bankAccount",e.target.value)}>
          <option value="1111">1111 – Banco Consorcio</option>
          <option value="1112">1112 – Consorcio Corredores de Bolsa</option>
          <option value="1110">1110 – Banco Cuenta Corriente</option>
          <option value="1100">1100 – Caja</option>
        </select>
      </div>
      <Field label="Notas"><textarea style={S.textarea} value={f.notes} onChange={e=>upd("notes",e.target.value)} placeholder="Condiciones, garantías…"/></Field>

      {/* ── Refinanciamiento desde saldo contable ── */}
      {!initial&&<div style={{marginBottom:16}}>
        <div style={{display:"flex",alignItems:"center",gap:10,marginBottom:8}}>
          <input type="checkbox" id="refiChk" checked={refiEnabled} onChange={e=>setRefiEnabled(e.target.checked)} style={{width:15,height:15,cursor:"pointer"}}/>
          <label htmlFor="refiChk" style={{...S.label,marginBottom:0,cursor:"pointer",fontWeight:700}}>
            ¿Este préstamo refinancia un saldo existente en contabilidad?
          </label>
        </div>
        {refiEnabled&&<div style={{border:`1px solid #7c3aed`,borderRadius:4,padding:16,background:"#faf5ff"}}>
          <div style={{fontSize:10,fontWeight:700,letterSpacing:2,textTransform:"uppercase",color:"#7c3aed",marginBottom:12}}>Saldo a refinanciar</div>

          {/* Counterparty selector — drives ALL account lines */}
          <div style={{display:"flex",gap:12,flexWrap:"wrap",marginBottom:12}}>
            <div style={{flex:1,minWidth:200}}>
              <label style={S.label}>Acreedor {availableCounterparties.length===0&&<span style={{color:C.muted,fontWeight:400}}>— sin saldos de pasivo en asientos</span>}</label>
              <select style={S.select} value={refiCounterparty} onChange={e=>setRefiCounterparty(e.target.value)}>
                <option value="">Seleccionar acreedor…</option>
                {availableCounterparties.map(cp=><option key={cp} value={cp}>{cp}</option>)}
              </select>
            </div>
            <div style={{flex:"0 0 160px"}}>
              <label style={S.label}>Fecha del asiento</label>
              <input style={S.input} type="date" value={refiDate} onChange={e=>setRefiDate(e.target.value)}/>
            </div>
          </div>

          {/* Show all account lines for this counterparty */}
          {refiCounterparty&&refiLines.length===0&&<div style={{background:"#f3f4f6",borderRadius:3,padding:"8px 12px",marginBottom:12,fontSize:12,color:C.muted}}>Sin saldos de pasivo para "{refiCounterparty}".</div>}
          {refiLines.length>0&&<div style={{background:"#ede9fe",borderRadius:3,padding:"10px 14px",marginBottom:12}}>
            <div style={{fontSize:10,fontWeight:700,letterSpacing:1,textTransform:"uppercase",color:"#6d28d9",marginBottom:8}}>Saldos encontrados — se extinguen todos</div>
            {refiLines.map(v=><div key={v.account} style={{display:"flex",justifyContent:"space-between",padding:"4px 0",borderBottom:`1px solid #c4b5fd`,fontSize:12.5}}>
              <span><code style={{fontSize:10,color:"#7c3aed",marginRight:6}}>{v.account}</code>{v.counterparty}</span>
              <span style={{fontWeight:700,color:"#4c1d95"}}>{fmtCLP(Math.round(v.balance))}</span>
            </div>)}
            <div style={{display:"flex",justifyContent:"space-between",paddingTop:8,fontWeight:700,fontSize:13}}>
              <span style={{color:"#4c1d95"}}>Total a extinguir</span>
              <span style={{color:"#4c1d95"}}>{fmtCLP(Math.round(selectedBalance))}</span>
            </div>
          </div>}

          {/* Cash diff */}
          {refiLines.length>0&&<div style={{display:"flex",gap:12,flexWrap:"wrap",marginBottom:12}}>
            <div style={{flex:"0 0 200px"}}>
              <label style={S.label}>Diferencia de caja (+entra / −sale)</label>
              <input style={S.input} type="number" step="1" placeholder="0" value={refiCashDiff} onChange={e=>setRefiCashDiff(e.target.value)}/>
            </div>
            {refiCash!==0&&<div style={{flex:"0 0 160px"}}>
              <label style={S.label}>Cuenta caja</label>
              <select style={S.select} value={refiCashAcc} onChange={e=>setRefiCashAcc(e.target.value)}>
                {["1111","1112","1110","1100"].map(x=><option key={x} value={x}>{x}</option>)}
              </select>
            </div>}
          </div>}

          {/* Preview asiento */}
          {refiLines.length>0&&<div style={{background:"#fff",border:`1px solid #c4b5fd`,borderRadius:3,padding:"10px 14px"}}>
            <div style={{fontSize:9.5,fontWeight:700,letterSpacing:2,textTransform:"uppercase",color:"#7c3aed",marginBottom:8}}>Asiento que se generará</div>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <thead><tr>
                <th style={{...S.th,fontSize:9}}>Cuenta</th>
                <th style={{...S.th,textAlign:"right",fontSize:9}}>Débito</th>
                <th style={{...S.th,textAlign:"right",fontSize:9}}>Crédito</th>
              </tr></thead>
              <tbody>
                {refiLines.map(v=><tr key={v.account}>
                  <td style={S.td}><code style={{fontSize:10,color:C.muted}}>{v.account}</code> {refiCounterparty} — se extingue</td>
                  <td style={{...S.td,textAlign:"right",fontWeight:700}}>{fmtCLP(Math.round(v.balance))}</td>
                  <td style={{...S.td,textAlign:"right",color:C.border}}>—</td>
                </tr>)}
                {refiCash>0&&<tr><td style={S.td}><code style={{fontSize:10,color:C.muted}}>{refiCashAcc}</code> Entrada de caja</td>
                  <td style={{...S.td,textAlign:"right",fontWeight:700}}>{fmtCLP(Math.round(refiCash))}</td><td style={{...S.td,textAlign:"right",color:C.border}}>—</td></tr>}
                <tr style={{background:"#f5f3ff"}}><td style={S.td}><code style={{fontSize:10,color:C.muted}}>{f.accountingCode||"2400"}</code> Nuevo préstamo — {f.name||"…"}</td>
                  <td style={{...S.td,textAlign:"right",color:C.border}}>—</td>
                  <td style={{...S.td,textAlign:"right",fontWeight:700,color:"#4c1d95"}}>{fmtCLP(Math.round(newLoanAmt))}</td></tr>
                {refiCash<0&&<tr><td style={S.td}><code style={{fontSize:10,color:C.muted}}>{refiCashAcc}</code> Salida de caja</td>
                  <td style={{...S.td,textAlign:"right",color:C.border}}>—</td>
                  <td style={{...S.td,textAlign:"right",fontWeight:700}}>{fmtCLP(Math.round(Math.abs(refiCash)))}</td></tr>}
              </tbody>
            </table>
          </div>}
        </div>}
      </div>}

      {preview&&!isCustom&&<div style={{background:"#f8f6f1",border:`1px solid ${C.border}`,borderRadius:3,padding:"12px 16px",marginTop:14,display:"flex",gap:28,flexWrap:"wrap"}}>
        <div><div style={S.sLbl}>Cuota aprox.</div><div style={{fontFamily:"'Georgia',serif",fontWeight:700,fontSize:18}}>{fmtNum(preview[0]?.cuota,2)} <span style={{fontSize:11,color:C.muted}}>{f.currency}</span></div></div>
        <div><div style={S.sLbl}>Total intereses</div><div style={{fontFamily:"'Georgia',serif",fontWeight:700,fontSize:16,color:C.danger}}>{fmtNum(preview.reduce((s,r)=>s+r.interest,0),2)}</div></div>
        <div><div style={S.sLbl}>Costo total</div><div style={{fontFamily:"'Georgia',serif",fontWeight:700,fontSize:16}}>{fmtNum(preview.reduce((s,r)=>s+r.cuota,0),2)}</div></div>
        <div><div style={S.sLbl}>Vencimiento</div><div style={{fontFamily:"'Georgia',serif",fontWeight:700,fontSize:16}}>{fmtDate(preview[preview.length-1]?.date)}</div></div>
      </div>}

      {isCustom&&<div style={{marginTop:16}}>
        <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:10}}>
          <span style={{...S.sLbl,marginBottom:0}}>Tabla personalizada <span style={{fontWeight:400,textTransform:"none",letterSpacing:0,color:C.muted}}>(click en celda para editar)</span></span>
          <div style={{display:"flex",gap:8}}>
            <Btn sm v="gold" onClick={()=>setCustomRows(rs=>{const last=rs[rs.length-1]; return [...rs,{period:(last?.period||0)+1,date:last?.date?addMonths(last.date,1):today(),capital:0,interest:0,cuota:0,balance:0,paid:false}];})}>+ Cuota</Btn>
          </div>
        </div>
        {customRows.length===0?<div style={{textAlign:"center",padding:20,border:`1px dashed ${C.border}`,borderRadius:3,color:C.muted}}>Ingresa N° de cuotas y presiona "+ Cuota" o define plazo</div>
        :<div style={{overflowX:"auto"}}>
          <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
            <thead><tr>{["#","Vencimiento","Capital","Interés","Cuota total","Saldo",""].map((h,i)=><th key={i} style={{...S.th,textAlign:i>=2&&i<=5?"right":"left"}}>{h}</th>)}</tr></thead>
            <tbody>{customRows.map((row,i)=><tr key={i} style={{background:i%2===0?"#fafaf9":"#fff"}}>
              <td style={{...S.td,color:C.muted,fontWeight:700,fontSize:11}}>{row.period}</td>
              <td style={S.td}><input type="date" value={row.date||""} onChange={e=>updRow(i,"date",e.target.value)} style={{...S.input,padding:"3px 6px",fontSize:11.5,width:140}}/></td>
              <td style={{...S.td,textAlign:"right"}}><EditCell value={row.capital} onChange={v=>updRow(i,"capital",v)}/></td>
              <td style={{...S.td,textAlign:"right"}}><EditCell value={row.interest} onChange={v=>updRow(i,"interest",v)}/></td>
              <td style={{...S.td,textAlign:"right",fontWeight:700}}>{fmtNum(row.cuota,2)}</td>
              <td style={{...S.td,textAlign:"right"}}><EditCell value={row.balance} onChange={v=>updRow(i,"balance",v)}/></td>
              <td style={S.td}><button style={S.bsm("transparent",C.danger)} onClick={()=>setCustomRows(rs=>rs.filter((_,j)=>j!==i).map((r,j)=>({...r,period:j+1})))}>✕</button></td>
            </tr>)}</tbody>
          </table>
          {customSum&&<div style={{background:"#f8f6f1",borderRadius:3,padding:"10px 14px",marginTop:10,display:"flex",gap:24,flexWrap:"wrap"}}>
            <div><div style={S.sLbl}>Capital total</div><div style={{fontWeight:700}}>{fmtNum(customSum.totalCapital,2)} {f.currency}</div></div>
            <div><div style={S.sLbl}>Intereses total</div><div style={{fontWeight:700,color:C.danger}}>{fmtNum(customSum.totalInterest,2)}</div></div>
            <div><div style={S.sLbl}>Pago total</div><div style={{fontWeight:700}}>{fmtNum(customSum.totalCuota,2)}</div></div>
            <div><div style={S.sLbl}>Cuotas</div><div style={{fontWeight:700}}>{customRows.length}</div></div>
          </div>}
        </div>}
      </div>}

      <div style={{display:"flex",gap:10,marginTop:18}}>
        <Btn onClick={submit}>{initial?"Guardar cambios":"Registrar pasivo"}</Btn>
        {onCancel&&<Btn v="outline" onClick={onCancel}>Cancelar</Btn>}
      </div>
    </div>
  </div>;
}

function AmortTable({liability,onToggle}){
  const table=liability.amortTable||[];
  const paidN=table.filter(r=>r.paid).length;
  const paidCap=table.filter(r=>r.paid).reduce((s,r)=>s+r.capital,0);
  const pendCap=(liability.originalAmount||0)-paidCap;
  const next=table.find(r=>!r.paid);
  return <div>
    <div style={{display:"grid",gridTemplateColumns:"repeat(4,1fr)",gap:12,marginBottom:16}}>
      {[{label:"Cuotas pagadas",v:`${paidN}/${table.length}`},{label:"Capital pagado",v:`${fmtNum(paidCap,2)} ${liability.currency}`},{label:"Saldo pendiente",v:`${fmtNum(pendCap,2)} ${liability.currency}`,r:pendCap>0},{label:"Próximo venc.",v:next?fmtDate(next.date):"—"}].map((s,i)=>(
        <div key={i} style={{...S.stat,borderTopColor:s.r?C.danger:C.gold}}><div style={S.sLbl}>{s.label}</div><div style={{...S.sVal,fontSize:15,color:s.r?C.danger:C.navy}}>{s.v}</div></div>
      ))}
    </div>
    <div style={{overflowX:"auto",maxHeight:400,overflowY:"auto"}}>
      <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
        <thead style={{position:"sticky",top:0}}><tr>{["N°","Venc.","Capital","Interés","Cuota","Saldo","Estado",""].map((h,i)=><th key={i} style={{...S.th,textAlign:i>=2&&i<=5?"right":"left"}}>{h}</th>)}</tr></thead>
        <tbody>{table.map((row,i)=><tr key={i} style={{background:row.paid?"#f0fdf4":i%2===0?"#fafaf9":"#fff",opacity:row.paid?.8:1}}>
          <td style={{...S.td,color:C.muted,fontWeight:700}}>{row.period}</td>
          <td style={S.td}>{fmtDate(row.date)}</td>
          <td style={{...S.td,textAlign:"right"}}>{fmtNum(row.capital,2)}</td>
          <td style={{...S.td,textAlign:"right",color:C.danger}}>{fmtNum(row.interest,2)}</td>
          <td style={{...S.td,textAlign:"right",fontWeight:700}}>{fmtNum(row.cuota,2)}</td>
          <td style={{...S.td,textAlign:"right",color:row.balance===0?C.green:C.navy}}>{fmtNum(row.balance,2)}</td>
          <td style={S.td}><span style={S.pill(row.paid)}>{row.paid?"Pagada":"Pendiente"}</span></td>
          <td style={S.td}><button style={S.bsm(row.paid?"#f1f5f9":C.navy,row.paid?C.muted:C.gold)} onClick={()=>onToggle(liability.id,i)}>{row.paid?"Desmarcar":"✓ Pagar"}</button></td>
        </tr>)}</tbody>
      </table>
    </div>
  </div>;
}



// ════════════════════════════════════════════════════════════════
//  REFINANCING FORM
// ════════════════════════════════════════════════════════════════
function RefinancingForm({liabilities, rates, onSave, onCancel}){
  const [origId, setOrigId]   = useState("");
  const [amountToRefi, setAmountToRefi] = useState(""); // capital a refinanciar del pasivo original
  const [cashDiff, setCashDiff]   = useState("");       // diferencia de caja (+entrada / -salida)
  const [cashAcc, setCashAcc]     = useState("1111");   // cuenta caja para diferencia
  const [date, setDate]           = useState(today());
  const [err, setErr]             = useState("");

  const orig = liabilities.find(l=>l.id===origId)||null;

  // Saldo pendiente del pasivo original
  const origBalance = useMemo(()=>{
    if(!orig) return 0;
    const paid = (orig.amortTable||[]).filter(r=>r.paid).reduce((s,r)=>s+r.capital,0);
    return orig.originalAmount - paid;
  },[orig]);

  // Monto a refinanciar: default = saldo total
  const refiAmt = parseFloat(amountToRefi) || origBalance;
  const cash    = parseFloat(cashDiff) || 0; // positive = cash in, negative = cash out

  // New loan amount = refinanced capital ± cash difference
  const newLoanAmt = refiAmt + cash;

  function submit(){
    setErr("");
    if(!origId)            return setErr("Selecciona el pasivo a refinanciar.");
    if(refiAmt<=0)         return setErr("Monto a refinanciar debe ser mayor a cero.");
    if(refiAmt>origBalance+0.01) return setErr(`El monto (${fmtNum(refiAmt,2)}) supera el saldo pendiente (${fmtNum(origBalance,2)} ${orig.currency}).`);
    if(newLoanAmt<=0)      return setErr("El monto del nuevo préstamo resultante debe ser positivo.");
    onSave({ origId, refiAmt, cash, cashAcc, date, origBalance, orig, newLoanAmt });
  }

  const isPartial = refiAmt < origBalance - 0.01;

  return <div style={S.card}>
    <div style={S.cHead()}><span style={S.cTitle}>🔄 Refinanciamiento de Pasivo</span></div>
    <div style={S.cBody}>
      {err&&<Msg>{err}</Msg>}

      {/* Step 1: select original liability */}
      <div style={{...S.card,border:`1px solid ${C.gold}`,marginBottom:20}}>
        <div style={{...S.cHead(C.navy),padding:"10px 16px"}}><span style={{...S.cTitle,fontSize:11}}>PASO 1 — Pasivo a refinanciar</span></div>
        <div style={{padding:16}}>
          <Field label="Pasivo original *">
            <select style={S.select} value={origId} onChange={e=>setOrigId(e.target.value)}>
              <option value="">Seleccionar pasivo…</option>
              {liabilities.map(l=>{
                const paid=(l.amortTable||[]).filter(r=>r.paid).reduce((s,r)=>s+r.capital,0);
                const bal=l.originalAmount-paid;
                return <option key={l.id} value={l.id}>{l.name} — Saldo: {fmtNum(bal,2)} {l.currency}</option>;
              })}
            </select>
          </Field>
          {orig&&<div style={{marginTop:12,display:"grid",gridTemplateColumns:"1fr 1fr 1fr",gap:12}}>
            <div style={S.stat}><div style={S.sLbl}>Acreedor</div><div style={{fontFamily:"'Georgia',serif",fontWeight:700,fontSize:13}}>{orig.lender||"—"}</div></div>
            <div style={S.stat}><div style={S.sLbl}>Saldo pendiente</div><div style={{fontFamily:"'Georgia',serif",fontWeight:700,fontSize:13,color:C.danger}}>{fmtNum(origBalance,2)} {orig.currency}</div></div>
            <div style={S.stat}><div style={S.sLbl}>Cuotas restantes</div><div style={{fontFamily:"'Georgia',serif",fontWeight:700,fontSize:13}}>{(orig.amortTable||[]).filter(r=>!r.paid).length}</div></div>
          </div>}
          {orig&&<div style={{marginTop:14}}>
            <label style={S.label}>Capital a refinanciar en {orig.currency} (dejar vacío = saldo total)</label>
            <input style={{...S.input,maxWidth:200}} type="number" min="0" step="0.01"
              placeholder={fmtNum(origBalance,2)}
              value={amountToRefi} onChange={e=>setAmountToRefi(e.target.value)}/>
            {isPartial&&<div style={{marginTop:6,background:"#fef9c3",border:"1px solid #f59e0b",borderRadius:3,padding:"7px 12px",fontSize:12,color:"#92400e"}}>
              ⚠ Refinanciamiento parcial — quedará un saldo remanente de <b>{fmtNum(origBalance-refiAmt,2)} {orig.currency}</b> en el pasivo original.
            </div>}
          </div>}
        </div>
      </div>

      {/* Step 2: cash difference */}
      {orig&&<div style={{...S.card,border:`1px solid ${C.border}`,marginBottom:20}}>
        <div style={{...S.cHead(C.navy),padding:"10px 16px"}}><span style={{...S.cTitle,fontSize:11}}>PASO 2 — Diferencia de caja (opcional)</span></div>
        <div style={{padding:16}}>
          <div style={{fontSize:12,color:C.muted,marginBottom:12}}>
            Si el nuevo préstamo entrega más dinero del que se cancela, ingresa la diferencia como positivo (+). Si se paga una parte en caja para reducir la deuda, como negativo (−).
          </div>
          <div style={{display:"flex",gap:14,flexWrap:"wrap",alignItems:"flex-end"}}>
            <div style={{flex:"0 0 200px"}}>
              <label style={S.label}>Diferencia en {orig.currency} (+entrada / −salida)</label>
              <input style={S.input} type="number" step="0.01" placeholder="0"
                value={cashDiff} onChange={e=>setCashDiff(e.target.value)}/>
            </div>
            {cash!==0&&<div style={{flex:"0 0 200px"}}>
              <label style={S.label}>Cuenta de caja</label>
              <select style={S.select} value={cashAcc} onChange={e=>setCashAcc(e.target.value)}>
                <option value="1111">1111 — Banco Consorcio</option>
                <option value="1112">1112 — CCB</option>
                <option value="1110">1110 — Banco Cuenta Corriente</option>
                <option value="1100">1100 — Caja</option>
              </select>
            </div>}
          </div>
        </div>
      </div>}

      {/* Step 3: summary + date */}
      {orig&&<div style={{...S.card,border:`1px solid ${C.border}`,marginBottom:20}}>
        <div style={{...S.cHead(C.navy),padding:"10px 16px"}}><span style={{...S.cTitle,fontSize:11}}>PASO 3 — Resumen del asiento</span></div>
        <div style={{padding:16}}>
          <div style={{marginBottom:14,maxWidth:480}}>
            <label style={S.label}>Fecha del asiento de refinanciamiento</label>
            <input style={{...S.input,maxWidth:180}} type="date" value={date} onChange={e=>setDate(e.target.value)}/>
          </div>
          {/* Preview asiento */}
          <div style={{background:"#f8f6f1",borderRadius:3,padding:"14px 16px"}}>
            <div style={{fontSize:10,fontWeight:700,letterSpacing:2,textTransform:"uppercase",color:C.muted,marginBottom:10}}>Asiento que se generará</div>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12.5}}>
              <thead><tr>
                <th style={{...S.th,fontSize:9.5}}>Cuenta</th>
                <th style={{...S.th,textAlign:"right",fontSize:9.5}}>Débito</th>
                <th style={{...S.th,textAlign:"right",fontSize:9.5}}>Crédito</th>
              </tr></thead>
              <tbody>
                <tr style={{background:"#fff"}}>
                  <td style={S.td}><code style={{fontSize:11,color:C.muted}}>{orig.accountingCode||"2400"}</code> {orig.name} (cancela {fmtNum(refiAmt,2)} {orig.currency})</td>
                  <td style={{...S.td,textAlign:"right",fontWeight:700,color:C.navy}}>{fmtNum(refiAmt,2)}</td>
                  <td style={{...S.td,textAlign:"right",color:C.border}}>—</td>
                </tr>
                {cash>0&&<tr style={{background:"#fafaf9"}}>
                  <td style={S.td}><code style={{fontSize:11,color:C.muted}}>{cashAcc}</code> Entrada de caja</td>
                  <td style={{...S.td,textAlign:"right",fontWeight:700,color:C.navy}}>{fmtNum(cash,2)}</td>
                  <td style={{...S.td,textAlign:"right",color:C.border}}>—</td>
                </tr>}
                <tr style={{background:"#fff"}}>
                  <td style={S.td}><code style={{fontSize:11,color:C.muted}}>{orig.accountingCode||"2400"}</code> Nuevo préstamo ({fmtNum(newLoanAmt,2)} {orig.currency})</td>
                  <td style={{...S.td,textAlign:"right",color:C.border}}>—</td>
                  <td style={{...S.td,textAlign:"right",fontWeight:700,color:C.navy}}>{fmtNum(newLoanAmt,2)}</td>
                </tr>
                {cash<0&&<tr style={{background:"#fafaf9"}}>
                  <td style={S.td}><code style={{fontSize:11,color:C.muted}}>{cashAcc}</code> Salida de caja</td>
                  <td style={{...S.td,textAlign:"right",color:C.border}}>—</td>
                  <td style={{...S.td,textAlign:"right",fontWeight:700,color:C.navy}}>{fmtNum(Math.abs(cash),2)}</td>
                </tr>}
              </tbody>
              <tfoot><tr style={{background:C.navy}}>
                <td style={{...S.td,color:C.gold,fontWeight:700,fontSize:10,letterSpacing:2,textTransform:"uppercase"}}>Total</td>
                <td style={{...S.td,textAlign:"right",color:"#fff",fontWeight:700}}>{fmtNum(refiAmt+(cash>0?cash:0),2)}</td>
                <td style={{...S.td,textAlign:"right",color:"#fff",fontWeight:700}}>{fmtNum(newLoanAmt+(cash<0?Math.abs(cash):0),2)}</td>
              </tr></tfoot>
            </table>
            <div style={{marginTop:10,fontSize:11,color:C.muted}}>
              A continuación podrás configurar el nuevo pasivo con sus condiciones (tasa, plazo, sistema).
            </div>
          </div>
        </div>
      </div>}

      <div style={{display:"flex",gap:10,marginTop:4}}>
        <Btn onClick={submit} disabled={!orig}>Continuar → Configurar nuevo pasivo</Btn>
        <Btn v="outline" onClick={onCancel}>Cancelar</Btn>
      </div>
    </div>
  </div>;
}

function LiabilitiesSection({rates,userId,entityId,accounts,entries,setEntries}){
  const [liabilities,setLiabilities]=useState(()=>lsLoad("ac_liabilities",[]));
  const [view,setView]=useState("list"); const [sel,setSel]=useState(null); const [msg,setMsg]=useState(null);
  const [refiData,setRefiData]=useState(null); // holds {origId,refiAmt,cash,cashAcc,date,...} from RefinancingForm

  useEffect(()=>{ dbLoad("ac_liabilities", userId, entityId, "ac_liabilities",[]).then(setLiabilities); },[userId]);

  function persist(d,record,deleted){
    setLiabilities(d);
    if(record) dbUpsert("ac_liabilities", userId, entityId, record,"ac_liabilities",d);
    if(deleted) dbDelete("ac_liabilities", userId, deleted, "ac_liabilities", entityId, d);
  }
  async function handleSave(l){
    const u=liabilities.find(x=>x.id===l.id)?liabilities.map(x=>x.id===l.id?l:x):[...liabilities,l];
    persist(u,l,null);

    // ── Generate refinancing entry if requested ──
    const rm = l.refiMeta;
    if(rm?.refiEnabled && rm.refiAmount>0){
      const {refiLines,refiCounterparty,refiCash,refiCashAcc,refiDate,selectedBalance,newLoanAmt} = rm;
      const newAccCode = l.accountingCode||"2400";
      const entryRows=[];
      // One debit row per account line (2400 capital + 2440 reajuste + etc.)
      refiLines.forEach(v=>entryRows.push({id:genId(),account:v.account,debit:Math.round(v.balance),credit:0,counterparty:v.counterparty||""}));
      if(refiCash>0) entryRows.push({id:genId(),account:refiCashAcc,debit:Math.round(refiCash),credit:0,counterparty:""});
      entryRows.push({id:genId(),account:newAccCode,debit:0,credit:Math.round(newLoanAmt),counterparty:l.lender||""});
      if(refiCash<0) entryRows.push({id:genId(),account:refiCashAcc,debit:0,credit:Math.round(Math.abs(refiCash)),counterparty:""});

      const totD=entryRows.reduce((s,r)=>s+r.debit,0);
      const totC=entryRows.reduce((s,r)=>s+r.credit,0);
      const curEntries=lsLoad("ac_entries",[]);
      const n=curEntries.length+1;
      const entry={id:genId(),number:n,date:refiDate,
        description:`Refinanciamiento${refiCounterparty?" "+refiCounterparty:""} → ${l.name}`,
        reference:"Auto-Refinanciamiento",
        rows:entryRows,totalDebit:totD,totalCredit:totC,
        createdAt:new Date().toISOString()};
      const upd=[...curEntries,entry];
      setEntries(upd);
      await dbUpsertEntry(userId,entityId,entry,upd);
      setMsg({ok:true,text:`"${l.name}" guardado. Asiento de refinanciamiento N°${n} generado.`});
    } else {
      setMsg({ok:true,text:`"${l.name}" guardado.`});
    }

    setView("list"); setTimeout(()=>setMsg(null),5000);
  }
  function del(id){ if(!confirm("¿Eliminar pasivo?")) return; const u=liabilities.filter(x=>x.id!==id); persist(u,null,id); if(sel?.id===id){setSel(null);setView("list");} }

  async function handleRefiStep1(data){
    // data = { origId, refiAmt, cash, cashAcc, date, origBalance, orig, newLoanAmt }
    setRefiData(data);
    setView("refi-new"); // go to LiabilityForm pre-filled with new loan amount
  }

  async function handleRefiSave(newLiab){
    if(!refiData) return;
    const {origId,refiAmt,cash,cashAcc,date,origBalance,orig} = refiData;

    // 1. Update original liability: mark as partially/fully cancelled
    const isPartial = refiAmt < origBalance - 0.01;
    let updatedLiabilities;
    if(isPartial){
      // Reduce originalAmount to reflect remaining balance
      const remaining = origBalance - refiAmt;
      const updOrig = {...orig,
        originalAmount: remaining,
        amortTable: (orig.amortTable||[]).filter(r=>!r.paid).map((r,i,arr)=>({...r,
          capital: parseFloat((r.capital * remaining / origBalance).toFixed(4)),
          period: i+1,
        })),
        notes: (orig.notes?orig.notes+" | ":"") + `Refinanciado parcialmente ${fmtDate(date)}: ${fmtNum(refiAmt,2)} ${orig.currency} → ${newLiab.name}`
      };
      updatedLiabilities = liabilities.map(l=>l.id===origId?updOrig:l);
      updatedLiabilities = [...updatedLiabilities, newLiab];
      persist(updatedLiabilities, updOrig, null);
      dbUpsert("ac_liabilities", userId, entityId, newLiab, "ac_liabilities", updatedLiabilities);
    } else {
      // Mark all remaining cuotas as paid (fully cancelled)
      const updOrig = {...orig,
        amortTable: (orig.amortTable||[]).map(r=>({...r,paid:true})),
        notes: (orig.notes?orig.notes+" | ":"") + `Cancelado por refinanciamiento ${fmtDate(date)} → ${newLiab.name}`
      };
      updatedLiabilities = liabilities.map(l=>l.id===origId?updOrig:l);
      updatedLiabilities = [...updatedLiabilities, newLiab];
      persist(updatedLiabilities, updOrig, null);
      dbUpsert("ac_liabilities", userId, entityId, newLiab, "ac_liabilities", updatedLiabilities);
    }

    // 2. Generate accounting entry
    const origAccCode = orig.accountingCode || "2400";
    const newAccCode  = newLiab.accountingCode || "2400";
    const cur = orig.currency;
    const ufRate = rates["UF"]||37500;
    const toClp = v => cur==="CLP"?Math.round(v):cur==="UF"?Math.round(v*ufRate):cur==="USD"?Math.round(v*(rates["USD"]||950)):Math.round(v);
    const refiCLP = toClp(refiAmt);
    const cashCLP = toClp(Math.abs(cash));
    const newCLP  = toClp(refiData.newLoanAmt);

    const entryRows = [];
    // Debit side: cancel original debt
    entryRows.push({id:genId(), account:origAccCode, debit:refiCLP, credit:0, counterparty:orig.lender||""});
    // Cash in (new loan > old debt)
    if(cash>0) entryRows.push({id:genId(), account:cashAcc, debit:cashCLP, credit:0, counterparty:""});
    // Credit side: new loan
    entryRows.push({id:genId(), account:newAccCode, debit:0, credit:newCLP, counterparty:newLiab.lender||""});
    // Cash out (new loan < old debt, client pays difference)
    if(cash<0) entryRows.push({id:genId(), account:cashAcc, debit:0, credit:cashCLP, counterparty:""});

    const totalD = entryRows.reduce((s,r)=>s+r.debit,0);
    const totalC = entryRows.reduce((s,r)=>s+r.credit,0);
    const currentEntries = lsLoad("ac_entries"+(entityId?":"+entityId:""),[]);
    const n = currentEntries.length + 1;
    const newEntry = {
      id:genId(), number:n, date,
      description:`Refinanciamiento: ${orig.name} → ${newLiab.name}`,
      reference:"Auto-Refinanciamiento",
      rows:entryRows, totalDebit:totalD, totalCredit:totalC,
      createdAt:new Date().toISOString()
    };
    const updatedEntries = [...currentEntries, newEntry];
    setEntries(updatedEntries);
    await dbUpsertEntry(userId, entityId, newEntry, updatedEntries);

    setRefiData(null);
    setView("list");
    setMsg({ok:true, text:`✓ Refinanciamiento registrado. Asiento N°${n} generado.`});
    setTimeout(()=>setMsg(null),6000);
  }

  async function togglePaid(lid,idx){
    const liability=liabilities.find(l=>l.id===lid);
    const row=liability?.amortTable?.[idx];
    if(!row) return;
    const nowPaying=!row.paid;

    // ── 1. Update amortTable state ──
    let updatedLiab;
    const u=liabilities.map(l=>{
      if(l.id!==lid) return l;
      const t=l.amortTable.map((r,i)=>i===idx?{...r,paid:!r.paid}:r);
      updatedLiab={...l,amortTable:t};
      return updatedLiab;
    });
    setLiabilities(u);
    lsSave("ac_liabilities",u);
    dbUpsert("ac_liabilities", userId, entityId, updatedLiab, "ac_liabilities", u);
    if(sel?.id===lid) setSel(u.find(x=>x.id===lid));

    if(!nowPaying) return; // desmarcar: solo revierte estado, no toca asientos

    // ── 2. Generar asientos contables ──
    const isUF    = liability.currency==="UF";
    const ufNow   = rates["UF"]||37500;   // UF actual (proxy para el mes)
    const currency= liability.currency;
    const liabAccCode    = liability.accountingCode||"2400";
    const reajAccCode    = "2440";   // siempre Reajuste UF Deuda
    const interesAccCode = "5350";   // Intereses Pagados
    const cajAcc         = liability.bankAccount||"1111";

    // Convertir UF→CLP usando UF actual
    const toCLP = v => {
      if(currency==="CLP") return Math.round(v);
      if(currency==="UF")  return Math.round(v*ufNow);
      if(currency==="USD") return Math.round(v*(rates["USD"]||950));
      if(currency==="EUR") return Math.round(v*(rates["EUR"]||1030));
      return Math.round(v);
    };

    // Entries key con entityId (fix clave localStorage)
    const entriesKey = "ac_entries"+(entityId?":"+entityId:"");
    let curEntries = lsLoad(entriesKey, entries);

    // ── Asiento A: Reajuste UF del período (si es UF) ──
    // Saldo en UF antes de esta cuota × variación IPC del mes
    // Usamos la diferencia entre UF actual y UF inicio del período
    // El saldo de 2440 acumula reajustes previos — aquí solo registramos el del período
    if(isUF){
      // Saldo UF pendiente ANTES de esta cuota
      const paidCapBefore = liability.amortTable.slice(0,idx).filter(r=>r.paid).reduce((s,r)=>s+r.capital,0);
      const saldoUF = liability.originalAmount - paidCapBefore;

      // Variación UF del mes de la cuota: usamos IPC del mes como proxy
      // Si el crédito tiene ufInicio registrado usamos diferencia real, si no aproximamos con 0.3%
      const ufInicioPeriodo = liability.ufInicioPeriodo || (ufNow / 1.003);
      const varUF = ufNow - ufInicioPeriodo; // CLP por UF de variación
      const reajCLP = Math.round(saldoUF * varUF);

      if(reajCLP !== 0){
        const abs = Math.abs(reajCLP);
        const n = curEntries.length+1;
        const reajRows = reajCLP > 0
          ? [ // UF subió → gasto reajuste + aumenta pasivo reajuste
              {id:genId(),account:"5241",      debit:abs, credit:0,   counterparty:liability.lender||""},
              {id:genId(),account:reajAccCode, debit:0,   credit:abs, counterparty:liability.lender||""},
            ]
          : [ // UF bajó (deflación) → ingreso reajuste + baja pasivo
              {id:genId(),account:reajAccCode, debit:abs, credit:0,   counterparty:liability.lender||""},
              {id:genId(),account:"4300",      debit:0,   credit:abs, counterparty:liability.lender||""},
            ];
        const eA={
          id:genId(), number:n, date:row.date,
          description:`Reajuste UF período ${row.period} — ${liability.name} (${fmtNum(saldoUF,2)} UF × ΔUF ${fmtNum(varUF,2)})`,
          reference:"Auto-Reajuste-UF",
          rows:reajRows, totalDebit:abs, totalCredit:abs,
          createdAt:new Date().toISOString()
        };
        curEntries=[...curEntries,eA];
        setEntries(curEntries);
        await dbUpsertEntry(userId,entityId,eA,curEntries);
      }
    }

    // ── Asiento B: Pago de cuota (capital + interés → caja) ──
    const capitalCLP = toCLP(row.capital);
    const interesCLP = toCLP(row.interest);
    const cuotaCLP   = capitalCLP + interesCLP;

    if(cuotaCLP > 0){
      curEntries = lsLoad(entriesKey, entries); // reload after asiento A
      const n2 = curEntries.length+1;
      const pagoRows=[];
      // Para UF: el capital en CLP reduce el saldo 2400; el reajuste acumulado de 2440 también se paga
      if(capitalCLP>0)  pagoRows.push({id:genId(),account:liabAccCode,   debit:capitalCLP, credit:0, counterparty:liability.lender||""});
      if(isUF){
        // Al pagar cuota UF: también extinguir el reajuste de 2440 acumulado proporcional
        // Estimamos el saldo 2440 para este acreedor desde los asientos
        const saldo2440 = entries.filter(e=>e.reference==="Auto-Reajuste-UF")
          .flatMap(e=>e.rows)
          .filter(r=>r.account===reajAccCode && (r.counterparty===liability.lender||""))
          .reduce((s,r)=>(r.credit||0)-(r.debit||0)+s, 0);
        // Solo incluir si hay saldo positivo
        if(saldo2440>0){
          const reajProp = Math.round(saldo2440 * (row.capital / (liability.originalAmount||1)));
          if(reajProp>0) pagoRows.push({id:genId(),account:reajAccCode,debit:reajProp,credit:0,counterparty:liability.lender||""});
        }
      }
      if(interesCLP>0)  pagoRows.push({id:genId(),account:interesAccCode,debit:interesCLP, credit:0, counterparty:liability.lender||""});
      const totPago = pagoRows.reduce((s,r)=>s+r.debit,0);
      pagoRows.push({id:genId(),account:cajAcc,debit:0,credit:totPago,counterparty:liability.lender||""});

      const eB={
        id:genId(), number:n2, date:row.date,
        description:`Pago cuota ${row.period}/${liability.months} — ${liability.name}`,
        reference:"Auto-Pago-Cuota",
        rows:pagoRows, totalDebit:totPago, totalCredit:totPago,
        createdAt:new Date().toISOString()
      };
      curEntries=[...curEntries,eB];
      setEntries(curEntries);
      await dbUpsertEntry(userId,entityId,eB,curEntries);
      setMsg({ok:true,text:`✓ Cuota ${row.period} pagada.${isUF?" Asientos de reajuste UF y pago generados.":""}`});
      setTimeout(()=>setMsg(null),6000);
    }
  }
  const totalDebtCLP=useMemo(()=>liabilities.reduce((s,l)=>{const pending=(l.amortTable||[]).filter(r=>!r.paid).reduce((a,r)=>a+r.capital,0); return s+toCLP(pending,l.currency,rates);},0),[liabilities,rates]);

  if(view==="new") return <LiabilityForm onSave={handleSave} onCancel={()=>setView("list")} entries={entries}/>;
  if(view==="edit"&&sel) return <LiabilityForm onSave={handleSave} onCancel={()=>setView("detail")} initial={sel}/>;
  if(view==="refi") return <RefinancingForm liabilities={liabilities} rates={rates} onSave={handleRefiStep1} onCancel={()=>setView("list")}/>;
  if(view==="refi-new"&&refiData) return <div>
    <div style={{background:"#ede9fe",border:"1px solid #7c3aed",borderRadius:4,padding:"10px 16px",marginBottom:16,fontSize:12,color:"#4c1d95"}}>
      🔄 Configurando nuevo pasivo del refinanciamiento — Monto sugerido: <b>{fmtNum(refiData.newLoanAmt,2)} {refiData.orig.currency}</b>
    </div>
    <LiabilityForm
      onSave={handleRefiSave}
      onCancel={()=>{setRefiData(null);setView("list");}}
      initial={{name:`Refinanciamiento ${refiData.orig.name}`,lender:refiData.orig.lender||"",currency:refiData.orig.currency,originalAmount:String(refiData.newLoanAmt),annualRate:"",months:"",startDate:refiData.date,system:"frances",notes:"",tags:"",accountingCode:refiData.orig.accountingCode||"2400"}}
    />
  </div>;
  if(view==="detail"&&sel){
    const l=liabilities.find(x=>x.id===sel.id)||sel;
    return <div>
      <div style={{display:"flex",gap:10,marginBottom:18}}><Btn v="outline" onClick={()=>setView("list")}>← Volver</Btn><Btn onClick={()=>{setSel(l);setView("edit");}}>Editar</Btn><Btn v="danger" onClick={()=>del(l.id)}>Eliminar</Btn></div>
      <div style={S.card}>
        <div style={S.cHead()}><span style={S.cTitle}>{l.name}</span><span style={S.tag(l.system)}>{AMORT_SYSTEMS.find(s=>s.v===l.system)?.l||l.system}</span></div>
        <div style={S.cBody}>
          <div style={{...S.g4,marginBottom:14}}>{[{label:"Acreedor",v:l.lender||"—"},{label:"Moneda",v:l.currency},{label:"Monto original",v:`${fmtNum(l.originalAmount,2)} ${l.currency}`},{label:"Tasa anual",v:`${fmtNum(l.annualRate,2)}%`},{label:"Plazo",v:`${l.months} meses`},{label:"Inicio",v:fmtDate(l.startDate)},{label:"Vencimiento",v:fmtDate(l.amortTable?.[l.months-1]?.date)},{label:"Etiquetas",v:l.tags||"—"}].map((s,i)=><div key={i}><div style={S.sLbl}>{s.label}</div><div style={{fontFamily:"'Georgia',serif",fontWeight:700,fontSize:14}}>{s.v}</div></div>)}</div>
          {l.notes&&<div style={{background:"#f8f6f1",borderRadius:3,padding:"9px 12px",fontSize:12.5,color:C.muted,fontStyle:"italic",marginBottom:14}}>{l.notes}</div>}
          <hr style={S.divider}/>
          <div style={{...S.cTitle,marginBottom:14}}>Tabla de Desarrollo — {l.currency}</div>
          <AmortTable liability={l} onToggle={togglePaid}/>
        </div>
      </div>
    </div>;
  }
  return <div>
    {msg&&<Msg ok={msg.ok}>{msg.text}</Msg>}
    <StatGrid stats={[{label:"Pasivos activos",value:liabilities.length},{label:"Deuda total (CLP)",value:fmtCLP(totalDebtCLP)},{label:"Cuotas pendientes hoy",value:liabilities.reduce((s,l)=>{const tm=today().slice(0,7);return s+(l.amortTable||[]).filter(r=>!r.paid&&r.date.startsWith(tm)).length;},0)},{label:"Monedas",value:[...new Set(liabilities.map(l=>l.currency))].join(", ")||"—"}]}/>
    <div style={{display:"flex",gap:10,justifyContent:"flex-end",marginBottom:14}}>
      {liabilities.length>0&&<Btn v="gold" onClick={()=>setView("refi")}>🔄 Refinanciar</Btn>}
      <Btn onClick={()=>setView("new")}>+ Nuevo Pasivo</Btn>
    </div>
    {liabilities.length===0?<div style={{...S.card,...S.empty}}><div style={{fontSize:32,marginBottom:10}}>🏦</div><div style={{fontFamily:"'Georgia',serif",color:C.muted,marginBottom:14}}>Sin pasivos registrados</div><Btn onClick={()=>setView("new")}>Registrar primer pasivo</Btn></div>
    :<div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>Pasivos</span></div>
      <DataTable cols={[
        {label:"Nombre",fn:l=><b style={{cursor:"pointer",fontFamily:"'Georgia',serif"}} onClick={()=>{setSel(l);setView("detail");}}>{l.name}</b>},
        {label:"Acreedor",key:"lender"},{label:"Sistema",fn:l=><span style={S.tag(l.system)}>{l.system}</span>},{label:"Mon.",key:"currency"},
        {label:"Monto orig.",r:true,fn:l=><span>{fmtNum(l.originalAmount,2)}</span>},
        {label:"Saldo",r:true,fn:l=>{const paid=(l.amortTable||[]).filter(r=>r.paid).reduce((s,r)=>s+r.capital,0);const bal=l.originalAmount-paid;return<span style={{fontWeight:700,color:bal>0?C.danger:C.green}}>{fmtNum(bal,2)} {l.currency}</span>;}},
        {label:"Avance",fn:l=>{const p=Math.round((l.amortTable||[]).filter(r=>r.paid).length/(l.months||1)*100);return<div style={{display:"flex",alignItems:"center",gap:7}}><div style={{width:56,height:5,background:"#e2e8f0",borderRadius:3}}><div style={{width:`${p}%`,height:5,background:p===100?C.green:C.gold,borderRadius:3}}/></div><span style={{fontSize:10.5,fontWeight:700}}>{p}%</span></div>;}},
        {label:"",fn:l=><div style={{display:"flex",gap:6}}><button style={S.bsm(C.navy,C.gold)} onClick={()=>{setSel(l);setView("detail");}}>Ver</button><button style={S.bsm("transparent",C.danger)} onClick={()=>del(l.id)}>✕</button></div>},
      ]} rows={liabilities}/>
    </div>}
  </div>;
}



// ════════════════════════════════════════════════════════════════
//  INVESTMENTS SECTION
// ════════════════════════════════════════════════════════════════
const INV_TYPES=["Acción","Bono","Fondo","ETF","Otro"];
const INV_MOVES=[{v:"compra",l:"Compra"},{v:"venta",l:"Venta"},{v:"dividendo",l:"Dividendo"},{v:"cupon",l:"Cupón/Interés"},{v:"ajuste",l:"Ajuste"}];

function InvestmentsSection({rates,userId,entityId,accounts,entries,setEntries}){
  const [insts,setInsts]=useState(()=>lsLoad("ac_inv_instruments",[]));
  const [movs, setMovs] =useState(()=>lsLoad("ac_inv_movements",[]));
  const [mkt,  setMkt]  =useState(()=>lsLoad("ac_inv_market",{}));
  const [view,setView]=useState("list"); const [sel,setSel]=useState(null); const [msg,setMsg]=useState(null); const [err,setErr]=useState("");

  useEffect(()=>{
    if(!userId) return;
    dbLoad("ac_inv_instruments", userId, entityId, "ac_inv_instruments",[]).then(setInsts);
    dbLoad("ac_inv_movements", userId, entityId, "ac_inv_movements",[]).then(setMovs);
    dbLoad("ac_inv_market", userId, entityId, "ac_inv_market_rows",[]).then(rows=>{
      const map={};
      rows.forEach(r=>{ if(!map[r.instrumentId]) map[r.instrumentId]=[]; map[r.instrumentId].push(r); });
      Object.keys(map).forEach(k=>map[k].sort((a,b)=>b.date.localeCompare(a.date)));
      setMkt(map); lsSave("ac_inv_market",map);
    });
  },[userId]);

  // ── Generar asiento contable automático ──
  async function createEntry({date, description, entryRows}){
    const totalD=entryRows.reduce((s,r)=>s+r.debit,0);
    const totalC=entryRows.reduce((s,r)=>s+r.credit,0);
    const currentEntries = lsLoad("ac_entries",[]);
    const n = currentEntries.length + 1;
    const newEntry={id:genId(),number:n,date,description,reference:"Auto-Inversiones",rows:entryRows,totalDebit:totalD,totalCredit:totalC,createdAt:new Date().toISOString()};
    const updated=[...currentEntries,newEntry];
    setEntries(updated);
    await dbUpsertEntry(userId, entityId, newEntry, updated);
  }

  const eI={name:"",ticker:"",type:"Acción",currency:"USD",isin:"",custodian:"",notes:"",accountCode:""};
  const eM={instrumentId:"",type:"compra",date:today(),qty:"",unitPrice:"",fxRate:"",broker:"",notes:"",ref:"",comisionPct:"",extras:[]};
  const eMk={instrumentId:"",date:today(),price:"",source:""};
  const [iF,setIF]=useState(eI); const [mF,setMF]=useState(eM); const [mkF,setMkF]=useState(eMk);

  const portfolio=useMemo(()=>{
    const st={};
    insts.forEach(i=>{st[i.id]={qty:0,avgCost:0,totalCost:0,realizedPnL:0,dividends:0};});
    [...movs].sort((a,b)=>a.date.localeCompare(b.date)).forEach(m=>{
      if(!st[m.instrumentId]) return;
      const s=st[m.instrumentId],qty=parseFloat(m.qty)||0,price=parseFloat(m.unitPrice)||0;
      if(m.type==="compra"){const nq=s.qty+qty,nc=s.totalCost+qty*price;s.avgCost=nq>0?nc/nq:0;s.qty=nq;s.totalCost=nc;}
      else if(m.type==="venta"){s.realizedPnL+=qty*price-qty*s.avgCost;s.totalCost=Math.max(0,s.totalCost-qty*s.avgCost);s.qty=Math.max(0,s.qty-qty);}
      else if(m.type==="dividendo"||m.type==="cupon"){s.dividends+=qty*price;}
    });
    return st;
  },[insts,movs]);

  const latestMkt=id=>(mkt[id]||[])[0]||null;
  const unrealized=inst=>{const s=portfolio[inst.id]||{};const m=latestMkt(inst.id);return(m&&s.qty)?(m.price-s.avgCost)*s.qty:null;};
  const totalValCLP=useMemo(()=>insts.reduce((sum,inst)=>{const s=portfolio[inst.id]||{};const m=latestMkt(inst.id);return sum+toCLP(m?m.price*s.qty:s.totalCost,inst.currency,rates);},0),[insts,portfolio,mkt,rates]);
  const totalCostCLP=useMemo(()=>insts.reduce((sum,inst)=>sum+toCLP((portfolio[inst.id]||{}).totalCost||0,inst.currency,rates),0),[insts,portfolio,rates]);
  const totalUnrCLP=useMemo(()=>insts.reduce((sum,inst)=>{const u=unrealized(inst);return u!=null?sum+toCLP(u,inst.currency,rates):sum;},0),[insts,portfolio,mkt,rates]);

  function saveInst(){
    setErr(""); if(!iF.name.trim()) return setErr("Nombre obligatorio.");
    const inst={id:sel?.id||genId(),...iF,createdAt:sel?.createdAt||new Date().toISOString()};
    const u=insts.find(x=>x.id===inst.id)?insts.map(x=>x.id===inst.id?inst:x):[...insts,inst];
    setInsts(u); dbUpsert("ac_inv_instruments", userId, entityId, inst,"ac_inv_instruments",u);
    setMsg({ok:true,text:`"${inst.name}" guardado.`}); setView("list");setSel(null);setIF(eI);setTimeout(()=>setMsg(null),3000);
  }
  async function saveMov(){
    setErr(""); if(!mF.instrumentId) return setErr("Selecciona instrumento.");
    const qty=parseFloat(mF.qty),price=parseFloat(mF.unitPrice);
    if(!qty||qty<=0) return setErr("Cantidad inválida."); if(isNaN(price)||price<0) return setErr("Precio inválido.");
    if(mF.type==="venta"&&qty>(portfolio[mF.instrumentId]?.qty||0)) return setErr("Posición insuficiente.");

    // Intermediación
    const montoBase=qty*price;
    const inst=insts.find(x=>x.id===mF.instrumentId);
    const fx=parseFloat(mF.fxRate)||(inst?.currency==="CLP"?1:rates[inst?.currency]||1);
    const comisionMonto=montoBase*(parseFloat(mF.comisionPct)||0)/100;
    const extrasMonto=(mF.extras||[]).reduce((s,e)=>s+parseFloat(e.monto||0),0);
    const totalIntermOrig=comisionMonto+extrasMonto; // en moneda del instrumento (comision) + CLP (extras)
    const totalIntermCLP=Math.round(comisionMonto*(inst?.currency==="CLP"?1:fx)+extrasMonto);

    const m={id:genId(),...mF,qty,unitPrice:price,fxRate:parseFloat(mF.fxRate)||null,
      comisionPct:parseFloat(mF.comisionPct)||0,totalIntermCLP,extras:mF.extras||[],
      createdAt:new Date().toISOString()};
    const u=[...movs,m]; setMovs(u); dbUpsert("ac_inv_movements", userId, entityId, m,"ac_inv_movements",u);

    // ── Asiento contable automático ──
    const montoCLP=Math.round(qty*price*(inst?.currency==="CLP"?1:fx));
    const accCode=inst?.accountCode;

    if(accCode && montoCLP>0){
      const s=portfolio[mF.instrumentId]||{};
      const costoTotalCLP=Math.round(qty*s.avgCost*(inst?.currency==="CLP"?1:fx));
      const gpCLP=montoCLP-costoTotalCLP;
      const ticker=inst?.ticker||inst?.name||"Inversión";
      const descInterm=totalIntermCLP>0?` + interm. ${fmtCLP(totalIntermCLP)}`:"";

      if(mF.type==="compra"){
        const rows=[
          {id:genId(),account:accCode,debit:montoCLP+(totalIntermCLP),credit:0}, // capitalizar intermediación en compra
          {id:genId(),account:"1110",debit:0,credit:montoCLP+totalIntermCLP},
        ];
        await createEntry({date:mF.date,description:`Compra ${qty} ${ticker} @ ${price} ${inst?.currency}${descInterm}`,entryRows:rows});
      } else if(mF.type==="venta"){
        const rows=[];
        rows.push({id:genId(),account:"1110",debit:montoCLP,credit:0});
        if(gpCLP>=0){
          rows.push({id:genId(),account:accCode,debit:0,credit:costoTotalCLP});
          rows.push({id:genId(),account:"4300",debit:0,credit:gpCLP});
        } else {
          rows.push({id:genId(),account:"5300",debit:Math.abs(gpCLP),credit:0});
          rows.push({id:genId(),account:accCode,debit:0,credit:costoTotalCLP});
        }
        // Intermediación en venta = gasto directo
        if(totalIntermCLP>0){
          rows.push({id:genId(),account:"5310",debit:totalIntermCLP,credit:0});
          rows.push({id:genId(),account:"1110",debit:0,credit:totalIntermCLP});
        }
        await createEntry({date:mF.date,description:`Venta ${qty} ${ticker} @ ${price} ${inst?.currency}${descInterm}`,entryRows:rows});
      } else if(mF.type==="dividendo"||mF.type==="cupon"){
        const rows=[
          {id:genId(),account:"1110",debit:montoCLP,credit:0},
          {id:genId(),account:"4200",debit:0,credit:montoCLP},
        ];
        if(totalIntermCLP>0){
          rows.push({id:genId(),account:"5310",debit:totalIntermCLP,credit:0});
          rows.push({id:genId(),account:"1110",debit:0,credit:totalIntermCLP});
        }
        await createEntry({date:mF.date,description:`${mF.type==="dividendo"?"Dividendo":"Cupón"} ${ticker}${descInterm}`,entryRows:rows});
      }
    }

    setMsg({ok:true,text:`Movimiento registrado.${accCode?" Asiento contable generado.":""}`});
    setView(sel?"detail":"list");setMF(eM);setTimeout(()=>setMsg(null),4000);
  }
  async function saveMk(){
    setErr(""); if(!mkF.instrumentId) return setErr("Selecciona instrumento.");
    const price=parseFloat(mkF.price); if(isNaN(price)||price<0) return setErr("Precio inválido.");
    const entry={id:genId(),instrumentId:mkF.instrumentId,date:mkF.date,price,source:mkF.source,createdAt:new Date().toISOString()};
    const newMkt={...mkt,[mkF.instrumentId]:[...(mkt[mkF.instrumentId]||[]),entry].sort((a,b)=>b.date.localeCompare(a.date))};
    setMkt(newMkt); lsSave("ac_inv_market",newMkt); dbUpsert("ac_inv_market", userId, entityId, entry,"ac_inv_market_rows",[...Object.values(newMkt).flat()]);

    // ── Ajuste por precio de mercado ──
    const inst=insts.find(x=>x.id===mkF.instrumentId);
    const s=portfolio[mkF.instrumentId]||{};
    const accCode=inst?.accountCode;
    if(accCode && s.qty>0){
      const fx=inst?.currency==="CLP"?1:rates[inst?.currency]||1;
      const valorMktCLP=Math.round(price*s.qty*fx);
      const valorLibroCLP=Math.round(s.totalCost*fx);
      const diff=valorMktCLP-valorLibroCLP;
      if(Math.abs(diff)>1){
        const ticker=inst?.ticker||inst?.name||"Inversión";
        let entryRows;
        if(diff>0){
          entryRows=[
            {id:genId(),account:accCode,debit:diff,credit:0},
            {id:genId(),account:"4300",debit:0,credit:diff},
          ];
        } else {
          entryRows=[
            {id:genId(),account:"5300",debit:Math.abs(diff),credit:0},
            {id:genId(),account:accCode,debit:0,credit:Math.abs(diff)},
          ];
        }
        await createEntry({date:mkF.date,description:`Ajuste mercado ${ticker} @ ${price} ${inst?.currency}`,entryRows});
      }
    }
    setMsg({ok:true,text:`Precio registrado.${accCode&&s.qty>0?" Ajuste contable generado.":""}`});
    setView(sel?"detail":"list");setMkF(eMk);setTimeout(()=>setMsg(null),4000);
  }
  function delInst(id){
    if(!confirm("¿Eliminar instrumento?")) return;
    const u=insts.filter(x=>x.id!==id); setInsts(u); dbDelete("ac_inv_instruments", userId, id, "ac_inv_instruments", entityId, u);
    const um=movs.filter(m=>m.instrumentId!==id); setMovs(um); lsSave("ac_inv_movements",um);
    const mk2={...mkt};delete mk2[id];setMkt(mk2);lsSave("ac_inv_market",mk2);
    if(sel?.id===id){setSel(null);setView("list");}
  }
  if(view==="newInst"||view==="editInst") return <div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>{view==="editInst"?"Editar":"Nuevo"} Instrumento</span></div><div style={S.cBody}>
    {err&&<Msg>{err}</Msg>}
    <div style={{...S.g3,marginBottom:14}}><Inp label="Nombre *" placeholder="Apple Inc." value={iF.name} onChange={e=>setIF(f=>({...f,name:e.target.value}))}/><Inp label="Ticker" placeholder="AAPL" value={iF.ticker} onChange={e=>setIF(f=>({...f,ticker:e.target.value.toUpperCase()}))}/><Sel label="Tipo" options={INV_TYPES} value={iF.type} onChange={e=>setIF(f=>({...f,type:e.target.value}))}/></div>
    <div style={{...S.g3,marginBottom:14}}><Sel label="Moneda" options={CURRENCIES} value={iF.currency} onChange={e=>setIF(f=>({...f,currency:e.target.value}))}/><Inp label="ISIN" placeholder="US0378331005" value={iF.isin} onChange={e=>setIF(f=>({...f,isin:e.target.value.toUpperCase()}))}/><Inp label="Custodio / Broker" value={iF.custodian} onChange={e=>setIF(f=>({...f,custodian:e.target.value}))}/></div>
    <div style={{marginBottom:14}}>
      <label style={S.label}>Cuenta contable (para asientos automáticos)</label>
      <AccountSelect value={iF.accountCode||""} onChange={v=>setIF(f=>({...f,accountCode:v}))} accounts={accounts||[]}/>
      <div style={{fontSize:10.5,color:C.muted,marginTop:4}}>Selecciona la cuenta de inversiones asociada a este instrumento. Si no existe, créala primero en Plan de Cuentas.</div>
    </div>
    <Field label="Notas"><textarea style={S.textarea} value={iF.notes} onChange={e=>setIF(f=>({...f,notes:e.target.value}))}/></Field>
    <div style={{display:"flex",gap:10,marginTop:14}}><Btn onClick={saveInst}>Guardar</Btn><Btn v="outline" onClick={()=>{setView("list");setSel(null);setIF(eI);}}>Cancelar</Btn></div>
  </div></div>;

  if(view==="move"){ const si=insts.find(x=>x.id===mF.instrumentId); const isIn=mF.type==="compra"||mF.type==="dividendo"||mF.type==="cupon";
    const montoBase=(parseFloat(mF.qty)||0)*(parseFloat(mF.unitPrice)||0);
    const fx=parseFloat(mF.fxRate)||(si?.currency==="CLP"?1:rates[si?.currency]||1);
    const comisionM=montoBase*(parseFloat(mF.comisionPct)||0)/100;
    const extrasM=(mF.extras||[]).reduce((s,e)=>s+parseFloat(e.monto||0),0);
    const totalIntermCLP=Math.round(comisionM*fx+extrasM);
    return <div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>Registrar Movimiento</span></div><div style={S.cBody}>
      {err&&<Msg>{err}</Msg>}
      <div style={{...S.g3,marginBottom:14}}>
        <Field label="Instrumento *"><select style={S.select} value={mF.instrumentId} onChange={e=>setMF(f=>({...f,instrumentId:e.target.value}))}><option value="">Seleccionar…</option>{insts.map(i=><option key={i.id} value={i.id}>{i.ticker?`${i.ticker} — `:""}{i.name} ({i.currency})</option>)}</select></Field>
        <Sel label="Tipo *" options={INV_MOVES} value={mF.type} onChange={e=>setMF(f=>({...f,type:e.target.value}))}/>
        <Inp label="Fecha *" type="date" value={mF.date} onChange={e=>setMF(f=>({...f,date:e.target.value}))}/>
      </div>
      <div style={{...S.g4,marginBottom:14}}>
        <Inp label="Cantidad *" type="number" min="0" step="any" value={mF.qty} onChange={e=>setMF(f=>({...f,qty:e.target.value}))}/>
        <Inp label={`Precio unit. (${si?.currency||"—"}) *`} type="number" min="0" step="any" value={mF.unitPrice} onChange={e=>setMF(f=>({...f,unitPrice:e.target.value}))}/>
        {si&&si.currency!=="CLP"&&<Inp label={`TC → CLP (auto: ${fmtNum(rates[si.currency]||1,2)})`} type="number" min="0" step="any" value={mF.fxRate} onChange={e=>setMF(f=>({...f,fxRate:e.target.value}))}/>}
        <Inp label="Broker" value={mF.broker} onChange={e=>setMF(f=>({...f,broker:e.target.value}))}/>
      </div>
      {mF.instrumentId&&mF.qty&&mF.unitPrice&&<div style={{background:"#f8f6f1",borderRadius:3,padding:"9px 14px",marginBottom:12,fontSize:12.5,display:"flex",gap:22}}>
        <span>Monto: <b>{fmtNum(montoBase,2)} {si?.currency||""}</b></span>
        {si&&si.currency!=="CLP"&&<span>CLP: <b>{fmtCLP(montoBase*fx)}</b></span>}
        {!isIn&&<span style={{color:C.muted}}>Pos. actual: <b>{fmtNum(portfolio[mF.instrumentId]?.qty||0,4)}</b></span>}
      </div>}

      {/* Gastos de intermediación */}
      <div style={{background:"#f8fafc",border:`1px solid ${C.border}`,borderRadius:3,padding:"14px 16px",marginBottom:14}}>
        <div style={{...S.cTitle,fontSize:11,marginBottom:12,color:C.navy}}>Gastos de Intermediación</div>
        <div style={{display:"flex",gap:14,marginBottom:10,alignItems:"flex-end"}}>
          <div style={{width:180}}><Inp label={`Comisión % (${si?.currency||""})`} type="number" min="0" step="0.01" placeholder="0.00" value={mF.comisionPct} onChange={e=>setMF(f=>({...f,comisionPct:e.target.value}))}/></div>
          {(parseFloat(mF.comisionPct)||0)>0&&<div style={{paddingBottom:2,fontSize:12.5,color:C.muted}}>= {fmtNum(comisionM,2)} {si?.currency||""} ≈ {fmtCLP(comisionM*fx)}</div>}
        </div>
        <div style={{marginBottom:8}}>
          {(mF.extras||[]).map((ex,i)=><div key={i} style={{display:"flex",gap:10,marginBottom:8,alignItems:"flex-end"}}>
            <div style={{flex:2}}><Inp label={i===0?"Descripción (CLP)":""} placeholder="Custodia, impuesto…" value={ex.desc} onChange={e=>setMF(f=>({...f,extras:f.extras.map((x,j)=>j===i?{...x,desc:e.target.value}:x)}))}/></div>
            <div style={{width:140}}><Inp label={i===0?"Monto CLP":""} type="number" min="0" value={ex.monto} onChange={e=>setMF(f=>({...f,extras:f.extras.map((x,j)=>j===i?{...x,monto:e.target.value}:x)}))}/></div>
            <button style={{...S.bsm("transparent",C.danger),marginBottom:2}} onClick={()=>setMF(f=>({...f,extras:f.extras.filter((_,j)=>j!==i)}))}>✕</button>
          </div>)}
          <Btn sm v="outline" onClick={()=>setMF(f=>({...f,extras:[...(f.extras||[]),{desc:"",monto:""}]}))}>+ Agregar gasto</Btn>
        </div>
        {totalIntermCLP>0&&<div style={{background:"#fef9c3",borderRadius:3,padding:"6px 12px",fontSize:12,color:C.amber,fontWeight:700}}>Total intermediación: {fmtCLP(totalIntermCLP)}</div>}
      </div>

      <Inp label="Referencia" value={mF.ref} onChange={e=>setMF(f=>({...f,ref:e.target.value}))}/>
      <div style={{display:"flex",gap:10,marginTop:14}}><Btn onClick={saveMov}>Guardar</Btn><Btn v="outline" onClick={()=>setView(sel?"detail":"list")}>Cancelar</Btn></div>
    </div></div>; }


  if(view==="market") return <div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>Precio de Mercado</span></div><div style={S.cBody}>
    {err&&<Msg>{err}</Msg>}
    <div style={{...S.g4,marginBottom:14}}>
      <Field label="Instrumento *"><select style={S.select} value={mkF.instrumentId} onChange={e=>setMkF(f=>({...f,instrumentId:e.target.value}))}><option value="">Seleccionar…</option>{insts.map(i=><option key={i.id} value={i.id}>{i.ticker?`${i.ticker} — `:""}{i.name}</option>)}</select></Field>
      <Inp label="Fecha *" type="date" value={mkF.date} onChange={e=>setMkF(f=>({...f,date:e.target.value}))}/>
      <Inp label={`Precio (${insts.find(x=>x.id===mkF.instrumentId)?.currency||"local"}) *`} type="number" min="0" step="any" value={mkF.price} onChange={e=>setMkF(f=>({...f,price:e.target.value}))}/>
      <Inp label="Fuente" placeholder="Bloomberg, Yahoo…" value={mkF.source} onChange={e=>setMkF(f=>({...f,source:e.target.value}))}/>
    </div>
    {mkF.instrumentId&&mkF.price&&(()=>{ const s=portfolio[mkF.instrumentId]||{},price=parseFloat(mkF.price)||0,diff=price-s.avgCost; return<div style={{background:"#f8f6f1",borderRadius:3,padding:"9px 14px",marginBottom:12,fontSize:12.5,display:"flex",gap:22}}>
      <span>CP actual: <b>{fmtNum(s.avgCost,4)}</b></span>
      <span style={{color:diff>=0?C.green:C.danger,fontWeight:700}}>Dif.: {diff>=0?"+":""}{fmtNum(diff,4)} ({s.avgCost>0?((diff/s.avgCost)*100).toFixed(2):0}%)</span>
      {s.qty>0&&<span style={{color:diff>=0?C.green:C.danger}}>G/P no real.: <b>{diff>=0?"+":""}{fmtNum(diff*s.qty,2)}</b></span>}
    </div>; })()}
    <div style={{display:"flex",gap:10,marginTop:14}}><Btn onClick={saveMk}>Guardar precio</Btn><Btn v="outline" onClick={()=>setView(sel?"detail":"list")}>Cancelar</Btn></div>
  </div></div>;

  if(view==="detail"&&sel){ const inst=insts.find(x=>x.id===sel.id)||sel,s=portfolio[inst.id]||{},mk2=latestMkt(inst.id),unrl=unrealized(inst),mktH=mkt[inst.id]||[],instMovs=movs.filter(m=>m.instrumentId===inst.id).sort((a,b)=>b.date.localeCompare(a.date));
    return <div>
      <div style={{display:"flex",gap:10,marginBottom:16}}><Btn v="outline" onClick={()=>setView("list")}>← Volver</Btn><Btn onClick={()=>{setMF({...eM,instrumentId:inst.id});setView("move");}}>+ Movimiento</Btn><Btn v="gold" onClick={()=>{setMkF({...eMk,instrumentId:inst.id});setView("market");}}>📈 Precio mercado</Btn><Btn v="danger" onClick={()=>delInst(inst.id)}>Eliminar</Btn></div>
      <div style={S.card}>
        <div style={S.cHead()}><div style={{display:"flex",alignItems:"center",gap:14}}><span style={S.cTitle}>{inst.ticker?`${inst.ticker} — `:""}{inst.name}</span><span style={S.tag(inst.type)}>{inst.type}</span></div><span style={{color:"#94a3b8",fontSize:11}}>{inst.currency} · {inst.custodian||"—"}</span></div>
        <div style={S.cBody}>
          <div style={{display:"grid",gridTemplateColumns:"repeat(6,1fr)",gap:12,marginBottom:16}}>
            {[{label:"Posición",v:`${fmtNum(s.qty,4)} tít.`,r:s.qty<=0},{label:"Costo prom.",v:`${fmtNum(s.avgCost,4)} ${inst.currency}`},{label:"Costo total",v:`${fmtNum(s.totalCost,2)}`},{label:"Precio mkt.",v:mk2?`${fmtNum(mk2.price,4)}`:"—"},{label:"Valor mkt.",v:mk2&&s.qty?fmtNum(mk2.price*s.qty,2):"—"},{label:"G/P no real.",v:unrl!=null?`${unrl>=0?"+":""}${fmtNum(unrl,2)}`:"—",g:unrl>=0,r:unrl<0}].map((kp,i)=><div key={i} style={{...S.stat,borderTopColor:kp.r?C.danger:kp.g?C.green:C.gold}}><div style={S.sLbl}>{kp.label}</div><div style={{...S.sVal,fontSize:14,color:kp.r?C.danger:kp.g?C.green:C.navy}}>{kp.v}</div></div>)}
          </div>
          {mktH.length>0&&<><div style={{...S.cTitle,marginBottom:10,color:C.navy}}>Historial Precio Mercado</div>
          <div style={{overflowX:"auto",marginBottom:16}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <thead><tr>{["Fecha","Precio","vs CP","Dif. unit.","Valor posición","Fuente"].map((h,i)=><th key={i} style={{...S.th,textAlign:i>=1&&i<=4?"right":"left"}}>{h}</th>)}</tr></thead>
              <tbody>{mktH.map((m2,i)=>{const d=m2.price-s.avgCost,pct=s.avgCost>0?d/s.avgCost*100:0;return<tr key={i} style={{background:i%2===0?"#fafaf9":"#fff"}}>
                <td style={S.td}>{fmtDate(m2.date)}</td><td style={{...S.td,textAlign:"right",fontWeight:700}}>{fmtNum(m2.price,4)}</td>
                <td style={{...S.td,textAlign:"right",color:d>=0?C.green:C.danger}}>{d>=0?"+":""}{pct.toFixed(2)}%</td>
                <td style={{...S.td,textAlign:"right",color:d>=0?C.green:C.danger}}>{d>=0?"+":""}{fmtNum(d,4)}</td>
                <td style={{...S.td,textAlign:"right",fontWeight:700}}>{fmtNum(m2.price*s.qty,2)}</td>
                <td style={{...S.td,color:C.muted,fontSize:11}}>{m2.source||"—"}</td>
              </tr>;})}</tbody>
            </table>
          </div></>}
          <div style={{...S.cTitle,marginBottom:10,color:C.navy}}>Movimientos</div>
          {instMovs.length===0?<div style={S.empty}><div style={{color:C.muted}}>Sin movimientos</div></div>
          :<div style={{overflowX:"auto"}}>
            <table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <thead><tr>{["Fecha","Tipo","Cant.","Precio","Monto","TC","CLP","Broker"].map((h,i)=><th key={i} style={{...S.th,textAlign:i>=2&&i<=6?"right":"left"}}>{h}</th>)}</tr></thead>
              <tbody>{instMovs.map((m2,i)=>{const isIn=m2.type==="compra"||m2.type==="dividendo"||m2.type==="cupon";const total=(m2.qty||0)*(m2.unitPrice||0);const fx=m2.fxRate||rates[inst.currency]||1;const clp=inst.currency==="CLP"?total:total*fx;return<tr key={i} style={{background:i%2===0?"#fafaf9":"#fff"}}>
                <td style={S.td}>{fmtDate(m2.date)}</td>
                <td style={S.td}><span style={{background:isIn?C.greenBg:C.redBg,color:isIn?C.green:C.danger,padding:"1px 7px",borderRadius:12,fontSize:10,fontWeight:700,textTransform:"uppercase"}}>{m2.type}</span></td>
                <td style={{...S.td,textAlign:"right",fontWeight:700}}>{isIn?"+":"-"}{fmtNum(m2.qty,4)}</td>
                <td style={{...S.td,textAlign:"right"}}>{fmtNum(m2.unitPrice,4)}</td>
                <td style={{...S.td,textAlign:"right",fontWeight:700}}>{fmtNum(total,2)}</td>
                <td style={{...S.td,textAlign:"right",color:C.muted,fontSize:11}}>{m2.fxRate?fmtNum(m2.fxRate,2):"auto"}</td>
                <td style={{...S.td,textAlign:"right"}}>{fmtCLP(clp)}</td>
                <td style={{...S.td,color:C.muted,fontSize:11}}>{m2.broker||"—"}</td>
              </tr>;})}</tbody>
              <tfoot><tr style={{background:"#f1f5f9"}}>
                <td colSpan={2} style={{...S.td,fontWeight:700,fontSize:10,textTransform:"uppercase",letterSpacing:1}}>Resumen</td>
                <td style={{...S.td,textAlign:"right",fontWeight:700}}>Pos: {fmtNum(s.qty,4)}</td>
                <td style={{...S.td,textAlign:"right",fontWeight:700}}>CP: {fmtNum(s.avgCost,4)}</td>
                <td style={{...S.td,textAlign:"right",fontWeight:700}}>{fmtNum(s.totalCost,2)}</td>
                <td colSpan={2} style={{...S.td,textAlign:"right",fontWeight:700,color:s.realizedPnL>=0?C.green:C.danger}}>G/P Real.: {s.realizedPnL>=0?"+":""}{fmtNum(s.realizedPnL,2)}</td>
                <td style={{...S.td,textAlign:"right",color:C.muted,fontSize:11}}>Div.: {fmtNum(s.dividends,2)}</td>
              </tr></tfoot>
            </table>
          </div>}
        </div>
      </div>
    </div>; }

  return <div>
    {msg&&<Msg ok={msg.ok}>{msg.text}</Msg>}
    <StatGrid stats={[{label:"Instrumentos",value:insts.length},{label:"Valor cartera (CLP)",value:fmtCLP(totalValCLP)},{label:"G/P no realizada",value:(totalUnrCLP>=0?"+":"")+fmtCLP(totalUnrCLP),green:totalUnrCLP>0,danger:totalUnrCLP<0,sub:totalCostCLP>0?`${((totalUnrCLP/totalCostCLP)*100).toFixed(2)}% sobre costo`:""},{label:"Monedas",value:[...new Set(insts.map(i=>i.currency))].join(", ")||"—"}]}/>
    <div style={{display:"flex",gap:10,justifyContent:"flex-end",marginBottom:14}}>
      <Btn v="outline" onClick={()=>{setMF(eM);setSel(null);setView("move");}}>+ Movimiento</Btn>
      <Btn v="gold" onClick={()=>{setMkF(eMk);setSel(null);setView("market");}}>📈 Precio mercado</Btn>
      <Btn onClick={()=>{setIF(eI);setSel(null);setView("newInst");}}>+ Nuevo instrumento</Btn>
    </div>
    {insts.length===0?<div style={{...S.card,...S.empty}}><div style={{fontSize:32,marginBottom:10}}>📊</div><div style={{fontFamily:"'Georgia',serif",color:C.muted,marginBottom:14}}>Sin instrumentos registrados</div><Btn onClick={()=>setView("newInst")}>Registrar primer instrumento</Btn></div>
    :<div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>Cartera de Inversiones</span></div>
      <DataTable cols={[
        {label:"Ticker",fn:i=><code style={{fontSize:11,background:"#f1f5f9",padding:"2px 6px",borderRadius:3,fontWeight:700}}>{i.ticker||"—"}</code>},
        {label:"Nombre",fn:i=><b style={{cursor:"pointer",fontFamily:"'Georgia',serif"}} onClick={()=>{setSel(i);setView("detail");}}>{i.name}</b>},
        {label:"Tipo",fn:i=><span style={S.tag(i.type)}>{i.type}</span>},{label:"Mon.",key:"currency"},
        {label:"Posición",r:true,fn:i=><span style={{fontWeight:700,color:(portfolio[i.id]?.qty||0)<=0?C.muted:C.navy}}>{fmtNum(portfolio[i.id]?.qty||0,4)}</span>},
        {label:"Costo prom.",r:true,fn:i=><span>{fmtNum(portfolio[i.id]?.avgCost||0,4)}</span>},
        {label:"Costo total",r:true,fn:i=><span>{fmtNum(portfolio[i.id]?.totalCost||0,2)}</span>},
        {label:"Precio mkt.",r:true,fn:i=>{const m=latestMkt(i.id);return m?<span style={{fontWeight:700}}>{fmtNum(m.price,4)}</span>:<span style={{color:C.muted,fontSize:11}}>—</span>;}},
        {label:"Valor mkt.",r:true,fn:i=>{const s=portfolio[i.id]||{};const m=latestMkt(i.id);return m&&s.qty?<b style={{fontFamily:"'Georgia',serif"}}>{fmtNum(m.price*s.qty,2)}</b>:<span style={{color:C.muted}}>—</span>;}},
        {label:"G/P no real.",r:true,fn:i=>{const u=unrealized(i);return u!=null?<span style={{fontWeight:700,color:u>=0?C.green:C.danger}}>{u>=0?"+":""}{fmtNum(u,2)}</span>:<span style={{color:C.muted}}>—</span>;}},
        {label:"",fn:i=><button style={S.bsm(C.navy,C.gold)} onClick={()=>{setSel(i);setView("detail");}}>Ver</button>},
      ]} rows={insts}/>
    </div>}
  </div>;
}



// ════════════════════════════════════════════════════════════════
//  INVENTORY SECTION
// ════════════════════════════════════════════════════════════════
function InventorySection({rates,userId,entityId,accounts,entries,setEntries}){
  const [products,setProducts]=useState(()=>lsLoad("ac_inv_prods",[]));
  const [movs,setMovs]=useState(()=>lsLoad("ac_inv_movs",[]));
  const [mkt,setMkt]=useState(()=>lsLoad("ac_inv_mktprice",{}));
  const [view,setView]=useState("list"); const [sel,setSel]=useState(null); const [msg,setMsg]=useState(null); const [err,setErr]=useState("");

  useEffect(()=>{
    if(!userId) return;
    dbLoad("ac_inv_products", userId, entityId, "ac_inv_prods",[]).then(setProducts);
    dbLoad("ac_inv_product_movs", userId, entityId, "ac_inv_movs",[]).then(setMovs);
    dbLoad("ac_inv_product_market", userId, entityId, "ac_inv_mktprice_rows",[]).then(rows=>{
      const map={};
      rows.forEach(r=>{ if(!map[r.productId]) map[r.productId]=[]; map[r.productId].push(r); });
      Object.keys(map).forEach(k=>map[k].sort((a,b)=>b.date.localeCompare(a.date)));
      setMkt(map); lsSave("ac_inv_mktprice",map);
    });
  },[userId]);

  // ── Generar asiento contable ──
  async function createEntry({date,description,entryRows}){
    const totalD=entryRows.reduce((s,r)=>s+r.debit,0);
    const totalC=entryRows.reduce((s,r)=>s+r.credit,0);
    const cur=lsLoad("ac_entries",[]);
    const n=cur.length+1;
    const e={id:genId(),number:n,date,description,reference:"Auto-Inventario",rows:entryRows,totalDebit:totalD,totalCredit:totalC,createdAt:new Date().toISOString()};
    const updated=[...cur,e]; setEntries(updated); await dbUpsertEntry(userId, entityId, e, updated);
  }

  const eP={code:"",name:"",unit:"unidad",currency:"CLP",notes:"",accountCode:""};
  const eM={productId:"",type:"compra",date:today(),qty:"",unitCost:"",unitPrice:"",ref:"",notes:"",
    comisionPct:"",extras:[]}; // extras: [{desc,monto}]
  const eMk={productId:"",date:today(),price:"",source:""};
  const [pF,setPF]=useState(eP); const [mF,setMF]=useState(eM); const [mkF,setMkF]=useState(eMk);

  const inventoryState=useMemo(()=>{
    const st={};products.forEach(p=>{st[p.id]={qty:0,avgCost:0,totalCost:0};});
    [...movs].sort((a,b)=>a.date.localeCompare(b.date)).forEach(m=>{
      if(!st[m.productId]) return;
      const s=st[m.productId];
      if(m.type==="compra"||m.type==="entrada"){const nq=s.qty+(m.qty||0),nc=s.totalCost+(m.qty||0)*(m.unitCost||0);s.avgCost=nq>0?nc/nq:0;s.qty=nq;s.totalCost=nc;}
      else if(m.type==="venta"||m.type==="salida"){s.totalCost=Math.max(0,s.totalCost-(m.qty||0)*s.avgCost);s.qty=Math.max(0,s.qty-(m.qty||0));}
    });
    return st;
  },[products,movs]);

  const latestMkt=id=>(mkt[id]||[])[0]||null;
  const totalCLP=useMemo(()=>products.reduce((s,p)=>s+toCLP(inventoryState[p.id]?.totalCost||0,p.currency,rates),0),[products,inventoryState,rates]);

  function saveProd(){
    setErr(""); if(!pF.code.trim()||!pF.name.trim()) return setErr("Código y nombre obligatorios.");
    if(products.some(p=>p.code===pF.code.trim())) return setErr("Código ya existe.");
    const prod={id:genId(),...pF,code:pF.code.trim(),name:pF.name.trim(),createdAt:new Date().toISOString()};
    const u=[...products,prod]; setProducts(u); dbUpsert("ac_inv_products", userId, entityId, prod,"ac_inv_prods",u);
    setMsg({ok:true,text:`"${pF.name}" creado.`}); setView("list");setPF(eP);setTimeout(()=>setMsg(null),3000);
  }

  async function saveMov(){
    setErr(""); const qty=parseFloat(mF.qty),cost=parseFloat(mF.unitCost);
    if(!mF.productId) return setErr("Selecciona producto."); if(!qty||qty<=0) return setErr("Cantidad inválida.");
    const isIn=mF.type==="compra"||mF.type==="entrada";
    const isOut=mF.type==="venta"||mF.type==="salida";
    if(isIn&&(!cost||cost<0)) return setErr("Costo requerido.");
    const st=inventoryState[mF.productId]||{};
    if(isOut&&qty>st.qty) return setErr(`Stock insuficiente: ${fmtNum(st.qty,4)}`);

    // Calcular intermediación
    const montoBase=qty*(isIn?cost:parseFloat(mF.unitPrice)||0);
    const comisionMonto=montoBase*(parseFloat(mF.comisionPct)||0)/100;
    const extrasMonto=(mF.extras||[]).reduce((s,e)=>s+parseFloat(e.monto||0),0);
    const totalInterm=Math.round(comisionMonto+extrasMonto);

    const m={id:genId(),...mF,qty,unitCost:parseFloat(mF.unitCost)||0,unitPrice:parseFloat(mF.unitPrice)||0,
      comisionPct:parseFloat(mF.comisionPct)||0,totalInterm,extras:mF.extras||[],
      createdAt:new Date().toISOString()};
    const u=[...movs,m]; setMovs(u); dbUpsert("ac_inv_product_movs", userId, entityId, m,"ac_inv_movs",u);

    // ── Asientos contables ──
    const prod=products.find(x=>x.id===mF.productId);
    const accCode=prod?.accountCode;
    const montoCLP=Math.round(montoBase);
    const costoTotalCLP=Math.round(qty*st.avgCost);

    if(accCode && montoCLP>0){
      const nombre=prod.name;
      if(mF.type==="compra"){
        // Débito inventario / Crédito banco
        const rows=[
          {id:genId(),account:accCode,debit:montoCLP,credit:0},
          {id:genId(),account:"1110",debit:0,credit:montoCLP},
        ];
        // Intermediación va al costo del inventario (capitalizada)
        if(totalInterm>0) rows.push({id:genId(),account:accCode,debit:totalInterm,credit:0},{id:genId(),account:"1110",debit:0,credit:totalInterm});
        // Agrupar débitos del mismo acc
        const grouped={}; rows.forEach(r=>{if(!grouped[r.account])grouped[r.account]={debit:0,credit:0};grouped[r.account].debit+=r.debit;grouped[r.account].credit+=r.credit;});
        const finalRows=Object.entries(grouped).flatMap(([acc,v])=>{const out=[];if(v.debit>0)out.push({id:genId(),account:acc,debit:v.debit,credit:0});if(v.credit>0)out.push({id:genId(),account:acc,debit:0,credit:v.credit});return out;});
        await createEntry({date:mF.date,description:`Compra ${qty} ${nombre} @ ${fmtNum(cost,2)}${totalInterm>0?` + interm. ${fmtCLP(totalInterm)}`:""}`,entryRows:finalRows});
      } else if(mF.type==="venta"){
        const gpCLP=montoCLP-costoTotalCLP;
        const rows=[];
        rows.push({id:genId(),account:"1110",debit:montoCLP,credit:0}); // banco
        rows.push({id:genId(),account:"5100",debit:costoTotalCLP,credit:0}); // costo ventas
        rows.push({id:genId(),account:accCode,debit:0,credit:costoTotalCLP}); // baja inventario
        rows.push({id:genId(),account:"4100",debit:0,credit:montoCLP}); // ingreso ventas
        // Intermediación es gasto
        if(totalInterm>0){
          rows.push({id:genId(),account:"5310",debit:totalInterm,credit:0});
          rows.push({id:genId(),account:"1110",debit:0,credit:totalInterm});
        }
        await createEntry({date:mF.date,description:`Venta ${qty} ${nombre} @ ${fmtNum(parseFloat(mF.unitPrice)||0,2)}${totalInterm>0?` − interm. ${fmtCLP(totalInterm)}`:""}`,entryRows:rows});
      } else if(mF.type==="salida"){
        // Merma/ajuste: baja inventario contra gasto
        await createEntry({date:mF.date,description:`Merma/salida ${qty} ${nombre}`,entryRows:[
          {id:genId(),account:"5100",debit:costoTotalCLP,credit:0},
          {id:genId(),account:accCode,debit:0,credit:costoTotalCLP},
        ]});
      } else if(mF.type==="entrada"){
        // Devolución: sube inventario desde ingreso
        await createEntry({date:mF.date,description:`Devolución/entrada ${qty} ${nombre}`,entryRows:[
          {id:genId(),account:accCode,debit:montoCLP,credit:0},
          {id:genId(),account:"4110",debit:0,credit:montoCLP},
        ]});
      }
    }

    setMsg({ok:true,text:`Movimiento registrado.${accCode?" Asiento generado.":""}`});
    setView(sel?"detail":"list");setMF(eM);setTimeout(()=>setMsg(null),4000);
  }

  function saveMk(){
    setErr(""); if(!mkF.productId) return setErr("Selecciona producto.");
    const price=parseFloat(mkF.price); if(isNaN(price)||price<0) return setErr("Precio inválido.");
    const entry={id:genId(),productId:mkF.productId,date:mkF.date,price,source:mkF.source,createdAt:new Date().toISOString()};
    const newMkt={...mkt,[mkF.productId]:[...(mkt[mkF.productId]||[]),entry].sort((a,b)=>b.date.localeCompare(a.date))};
    setMkt(newMkt); lsSave("ac_inv_mktprice",newMkt); dbUpsert("ac_inv_product_market", userId, entityId, entry,"ac_inv_mktprice_rows",[...Object.values(newMkt).flat()]);
    setMsg({ok:true,text:"Precio registrado."}); setView(sel?"detail":"list");setMkF(eMk);setTimeout(()=>setMsg(null),3000);
  }
  function delProd(id){
    if(!confirm("¿Eliminar producto?")) return;
    const u=products.filter(p=>p.id!==id); setProducts(u); dbDelete("ac_inv_products", userId, id, "ac_inv_prods", entityId, u);
    const um=movs.filter(m=>m.productId!==id); setMovs(um); lsSave("ac_inv_movs",um);
    const mk2={...mkt};delete mk2[id];setMkt(mk2);lsSave("ac_inv_mktprice",mk2);
    if(sel?.id===id){setSel(null);setView("list");}
  }

  if(view==="newProd") return <div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>Nuevo Producto</span></div><div style={S.cBody}>
    {err&&<Msg>{err}</Msg>}
    <div style={{...S.g4,marginBottom:14}}>
      <Inp label="Código *" placeholder="PROD-001" value={pF.code} onChange={e=>setPF(f=>({...f,code:e.target.value}))}/>
      <Inp label="Nombre *" value={pF.name} onChange={e=>setPF(f=>({...f,name:e.target.value}))}/>
      <Inp label="Unidad" placeholder="unidad, kg, litro" value={pF.unit} onChange={e=>setPF(f=>({...f,unit:e.target.value}))}/>
      <Sel label="Moneda" options={CURRENCIES} value={pF.currency} onChange={e=>setPF(f=>({...f,currency:e.target.value}))}/>
    </div>
    <div style={{marginBottom:14}}>
      <label style={S.label}>Cuenta contable (para asientos automáticos)</label>
      <AccountSelect value={pF.accountCode||""} onChange={v=>setPF(f=>({...f,accountCode:v}))} accounts={accounts||[]}/>
      <div style={{fontSize:10.5,color:C.muted,marginTop:4}}>Cuenta de inventario de este producto. Ej: 1300 Inventario de Mercaderías.</div>
    </div>
    <Field label="Notas"><textarea style={S.textarea} value={pF.notes} onChange={e=>setPF(f=>({...f,notes:e.target.value}))}/></Field>
    <div style={{display:"flex",gap:10,marginTop:14}}><Btn onClick={saveProd}>Crear</Btn><Btn v="outline" onClick={()=>{setView("list");setPF(eP);}}>Cancelar</Btn></div>
  </div></div>;

  if(view==="move"){ const sp=products.find(x=>x.id===mF.productId);const isIn=mF.type==="compra"||mF.type==="entrada";
    const montoBase=(parseFloat(mF.qty)||0)*parseFloat(isIn?mF.unitCost:mF.unitPrice||0);
    const comisionM=montoBase*(parseFloat(mF.comisionPct)||0)/100;
    const extrasM=(mF.extras||[]).reduce((s,e)=>s+parseFloat(e.monto||0),0);
    const totalInterm=comisionM+extrasM;
    return <div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>Registrar Movimiento</span></div><div style={S.cBody}>
      {err&&<Msg>{err}</Msg>}
      <div style={{...S.g3,marginBottom:14}}>
        <Field label="Producto *"><select style={S.select} value={mF.productId} onChange={e=>setMF(f=>({...f,productId:e.target.value}))}><option value="">Seleccionar…</option>{products.map(p=><option key={p.id} value={p.id}>{p.code} — {p.name}</option>)}</select></Field>
        <Sel label="Tipo *" options={[{v:"compra",l:"Compra"},{v:"venta",l:"Venta"},{v:"entrada",l:"Entrada/Devolución"},{v:"salida",l:"Salida/Merma"}]} value={mF.type} onChange={e=>setMF(f=>({...f,type:e.target.value}))}/>
        <Inp label="Fecha *" type="date" value={mF.date} onChange={e=>setMF(f=>({...f,date:e.target.value}))}/>
      </div>
      <div style={{...S.g4,marginBottom:14}}>
        <Inp label="Cantidad *" type="number" min="0" step="any" value={mF.qty} onChange={e=>setMF(f=>({...f,qty:e.target.value}))}/>
        <Inp label={isIn?"Costo unit. *":"Costo unit."} type="number" min="0" step="any" value={mF.unitCost} onChange={e=>setMF(f=>({...f,unitCost:e.target.value}))}/>
        {!isIn&&<Inp label="Precio venta unit." type="number" min="0" step="any" value={mF.unitPrice} onChange={e=>setMF(f=>({...f,unitPrice:e.target.value}))}/>}
        <Inp label="Referencia" value={mF.ref} onChange={e=>setMF(f=>({...f,ref:e.target.value}))}/>
      </div>
      {mF.productId&&!isIn&&<div style={{background:"#f8f6f1",borderRadius:3,padding:"9px 14px",marginBottom:12,fontSize:12.5}}>Stock: <b>{fmtNum(inventoryState[mF.productId]?.qty||0,4)}</b> — CP: <b>{fmtNum(inventoryState[mF.productId]?.avgCost||0,4)}</b></div>}

      {/* Gastos de intermediación */}
      <div style={{background:"#f8fafc",border:`1px solid ${C.border}`,borderRadius:3,padding:"14px 16px",marginBottom:14}}>
        <div style={{...S.cTitle,fontSize:11,marginBottom:12,color:C.navy}}>Gastos de Intermediación</div>
        <div style={{display:"flex",gap:14,marginBottom:10,alignItems:"flex-end"}}>
          <div style={{width:180}}><Inp label="Comisión %" type="number" min="0" step="0.01" placeholder="0.00" value={mF.comisionPct} onChange={e=>setMF(f=>({...f,comisionPct:e.target.value}))}/></div>
          {(parseFloat(mF.comisionPct)||0)>0&&<div style={{paddingBottom:2,fontSize:12.5,color:C.muted}}>= {fmtCLP(comisionM)}</div>}
        </div>
        <div style={{marginBottom:8}}>
          {(mF.extras||[]).map((ex,i)=><div key={i} style={{display:"flex",gap:10,marginBottom:8,alignItems:"flex-end"}}>
            <div style={{flex:2}}><Inp label={i===0?"Descripción":""} placeholder="Flete, seguro…" value={ex.desc} onChange={e=>setMF(f=>({...f,extras:f.extras.map((x,j)=>j===i?{...x,desc:e.target.value}:x)}))}/></div>
            <div style={{width:140}}><Inp label={i===0?"Monto CLP":""} type="number" min="0" value={ex.monto} onChange={e=>setMF(f=>({...f,extras:f.extras.map((x,j)=>j===i?{...x,monto:e.target.value}:x)}))}/></div>
            <button style={{...S.bsm("transparent",C.danger),marginBottom:2}} onClick={()=>setMF(f=>({...f,extras:f.extras.filter((_,j)=>j!==i)}))}>✕</button>
          </div>)}
          <Btn sm v="outline" onClick={()=>setMF(f=>({...f,extras:[...(f.extras||[]),{desc:"",monto:""}]}))}>+ Agregar gasto</Btn>
        </div>
        {totalInterm>0&&<div style={{background:"#fef9c3",borderRadius:3,padding:"6px 12px",fontSize:12,color:C.amber,fontWeight:700}}>Total intermediación: {fmtCLP(totalInterm)}</div>}
      </div>

      {montoBase>0&&<div style={{background:"#f8f6f1",borderRadius:3,padding:"9px 14px",marginBottom:12,fontSize:12.5,display:"flex",gap:22}}>
        <span>Monto base: <b>{fmtCLP(montoBase)}</b></span>
        {totalInterm>0&&<span>Intermediación: <b style={{color:C.danger}}>{fmtCLP(totalInterm)}</b></span>}
        <span>Total: <b>{fmtCLP(montoBase+(isIn?totalInterm:0))}</b></span>
      </div>}
      <div style={{display:"flex",gap:10,marginTop:14}}><Btn onClick={saveMov}>Guardar</Btn><Btn v="outline" onClick={()=>setView(sel?"detail":"list")}>Cancelar</Btn></div>
    </div></div>; }

  if(view==="market") return <div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>Precio de Mercado</span></div><div style={S.cBody}>
    {err&&<Msg>{err}</Msg>}
    <div style={{...S.g4,marginBottom:14}}>
      <Field label="Producto *"><select style={S.select} value={mkF.productId} onChange={e=>setMkF(f=>({...f,productId:e.target.value}))}><option value="">Seleccionar…</option>{products.map(p=><option key={p.id} value={p.id}>{p.code} — {p.name}</option>)}</select></Field>
      <Inp label="Fecha *" type="date" value={mkF.date} onChange={e=>setMkF(f=>({...f,date:e.target.value}))}/>
      <Inp label="Precio *" type="number" min="0" step="any" value={mkF.price} onChange={e=>setMkF(f=>({...f,price:e.target.value}))}/>
      <Inp label="Fuente" value={mkF.source} onChange={e=>setMkF(f=>({...f,source:e.target.value}))}/>
    </div>
    <div style={{display:"flex",gap:10,marginTop:14}}><Btn onClick={saveMk}>Guardar</Btn><Btn v="outline" onClick={()=>setView(sel?"detail":"list")}>Cancelar</Btn></div>
  </div></div>;

  if(view==="detail"&&sel){ const p=products.find(x=>x.id===sel.id)||sel,st=inventoryState[p.id]||{},mk2=latestMkt(p.id),mktH=mkt[p.id]||[],pMovs=movs.filter(m=>m.productId===p.id).sort((a,b)=>b.date.localeCompare(a.date)),unrl=mk2?(mk2.price-st.avgCost)*st.qty:null;
    return <div>
      <div style={{display:"flex",gap:10,marginBottom:16}}><Btn v="outline" onClick={()=>setView("list")}>← Volver</Btn><Btn onClick={()=>{setMF({...eM,productId:p.id});setView("move");}}>+ Movimiento</Btn><Btn v="gold" onClick={()=>{setMkF({...eMk,productId:p.id});setView("market");}}>📈 Precio mercado</Btn><Btn v="danger" onClick={()=>delProd(p.id)}>Eliminar</Btn></div>
      <div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>{p.code} — {p.name}</span>
        <div style={{display:"flex",gap:10,alignItems:"center"}}>
          {p.accountCode&&<span style={{fontSize:11,color:C.gold}}>⚖ {p.accountCode}</span>}
          <span style={{color:"#94a3b8",fontSize:11}}>{p.unit} · {p.currency}</span>
        </div>
      </div>
        <div style={S.cBody}>
          <div style={{display:"grid",gridTemplateColumns:"repeat(5,1fr)",gap:12,marginBottom:16}}>
            {[{label:"Stock actual",v:`${fmtNum(st.qty,4)} ${p.unit}`,r:st.qty<=0},{label:"Costo promedio",v:`${fmtNum(st.avgCost,4)} ${p.currency}`},{label:"Valor libro",v:`${fmtNum(st.totalCost,2)} ${p.currency}`,bold:true},{label:"Precio mercado",v:mk2?`${fmtNum(mk2.price,4)} ${p.currency}`:"—"},{label:"G/P no realiz.",v:unrl!=null?`${unrl>=0?"+":""}${fmtNum(unrl,2)}`:"—",g:unrl>=0,r:unrl<0}].map((s,i)=><div key={i} style={{...S.stat,borderTopColor:s.r?C.danger:s.g?C.green:C.gold}}><div style={S.sLbl}>{s.label}</div><div style={{...S.sVal,fontSize:14,color:s.r?C.danger:s.g?C.green:C.navy}}>{s.v}</div></div>)}
          </div>
          {mktH.length>0&&<><div style={{...S.cTitle,marginBottom:10,color:C.navy}}>Historial Precio Mercado</div>
            <div style={{overflowX:"auto",marginBottom:16}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
              <thead><tr>{["Fecha","Precio","vs CP","Diferencia","Fuente"].map((h,i)=><th key={i} style={{...S.th,textAlign:i>=1&&i<=3?"right":"left"}}>{h}</th>)}</tr></thead>
              <tbody>{mktH.map((m2,i)=>{const d=m2.price-st.avgCost,pct=st.avgCost>0?d/st.avgCost*100:0;return<tr key={i} style={{background:i%2===0?"#fafaf9":"#fff"}}>
                <td style={S.td}>{fmtDate(m2.date)}</td><td style={{...S.td,textAlign:"right",fontWeight:700}}>{fmtNum(m2.price,4)}</td>
                <td style={{...S.td,textAlign:"right",color:d>=0?C.green:C.danger}}>{d>=0?"+":""}{pct.toFixed(2)}%</td>
                <td style={{...S.td,textAlign:"right",color:d>=0?C.green:C.danger}}>{d>=0?"+":""}{fmtNum(d,4)}</td>
                <td style={{...S.td,color:C.muted,fontSize:11}}>{m2.source||"—"}</td>
              </tr>;})}</tbody>
            </table></div></>}
          <div style={{...S.cTitle,marginBottom:10,color:C.navy}}>Movimientos</div>
          {pMovs.length===0?<div style={S.empty}><div style={{color:C.muted}}>Sin movimientos</div></div>
          :<div style={{overflowX:"auto"}}><table style={{width:"100%",borderCollapse:"collapse",fontSize:12}}>
            <thead><tr>{["Fecha","Tipo","Cant.","Costo unit.","Precio vta.","Intermediación","Margen","Ref."].map((h,i)=><th key={i} style={{...S.th,textAlign:i>=2&&i<=6?"right":"left"}}>{h}</th>)}</tr></thead>
            <tbody>{pMovs.map((m2,i)=>{const isIn=m2.type==="compra"||m2.type==="entrada";const margin=m2.unitPrice&&m2.unitCost?((m2.unitPrice-m2.unitCost)/m2.unitCost*100):null;return<tr key={i} style={{background:i%2===0?"#fafaf9":"#fff"}}>
              <td style={S.td}>{fmtDate(m2.date)}</td>
              <td style={S.td}><span style={{background:isIn?C.greenBg:C.redBg,color:isIn?C.green:C.danger,padding:"1px 7px",borderRadius:12,fontSize:10,fontWeight:700,textTransform:"uppercase"}}>{m2.type}</span></td>
              <td style={{...S.td,textAlign:"right",fontWeight:700}}>{isIn?"+":"-"}{fmtNum(m2.qty,4)}</td>
              <td style={{...S.td,textAlign:"right"}}>{m2.unitCost>0?fmtNum(m2.unitCost,4):"—"}</td>
              <td style={{...S.td,textAlign:"right"}}>{m2.unitPrice>0?fmtNum(m2.unitPrice,4):"—"}</td>
              <td style={{...S.td,textAlign:"right",color:C.danger}}>{m2.totalInterm>0?fmtCLP(m2.totalInterm):"—"}</td>
              <td style={{...S.td,textAlign:"right",color:margin>=0?C.green:C.danger}}>{margin!=null?`${margin.toFixed(1)}%`:"—"}</td>
              <td style={{...S.td,fontSize:11,color:C.muted}}>{m2.ref||"—"}</td>
            </tr>;})}</tbody>
          </table></div>}
        </div>
      </div>
    </div>; }

  return <div>
    {msg&&<Msg ok={msg.ok}>{msg.text}</Msg>}
    <StatGrid stats={[{label:"Productos",value:products.length},{label:"Valor inventario (CLP)",value:fmtCLP(totalCLP)},{label:"Movimientos",value:movs.length},{label:"Con precio mercado",value:Object.keys(mkt).filter(k=>(mkt[k]||[]).length>0).length}]}/>
    <div style={{display:"flex",gap:10,justifyContent:"flex-end",marginBottom:14}}>
      <Btn v="outline" onClick={()=>setView("move")}>+ Movimiento</Btn><Btn v="gold" onClick={()=>setView("market")}>📈 Precio mercado</Btn><Btn onClick={()=>setView("newProd")}>+ Nuevo producto</Btn>
    </div>
    {products.length===0?<div style={{...S.card,...S.empty}}><div style={{fontSize:32,marginBottom:10}}>📦</div><div style={{fontFamily:"'Georgia',serif",color:C.muted,marginBottom:14}}>Sin productos registrados</div><Btn onClick={()=>setView("newProd")}>Crear primer producto</Btn></div>
    :<div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>Inventario — Costo Promedio Ponderado</span></div>
      <DataTable cols={[
        {label:"Código",fn:p=><code style={{fontSize:11,background:"#f1f5f9",padding:"2px 6px",borderRadius:3}}>{p.code}</code>},
        {label:"Nombre",fn:p=><b style={{cursor:"pointer",fontFamily:"'Georgia',serif"}} onClick={()=>{setSel(p);setView("detail");}}>{p.name}</b>},
        {label:"Cuenta",fn:p=>p.accountCode?<code style={{fontSize:10,background:"#f1f5f9",padding:"1px 5px",borderRadius:3}}>{p.accountCode}</code>:<span style={{color:C.muted,fontSize:10}}>—</span>},
        {label:"Unidad",key:"unit"},
        {label:"Stock",r:true,fn:p=><span style={{fontWeight:700,color:(inventoryState[p.id]?.qty||0)<=0?C.danger:C.navy}}>{fmtNum(inventoryState[p.id]?.qty||0,4)}</span>},
        {label:"Costo prom.",r:true,fn:p=><span>{fmtNum(inventoryState[p.id]?.avgCost||0,4)}</span>},
        {label:"Valor libro",r:true,fn:p=><b style={{fontFamily:"'Georgia',serif"}}>{fmtNum(inventoryState[p.id]?.totalCost||0,2)}</b>},
        {label:"Precio mkt.",r:true,fn:p=>{const m=latestMkt(p.id);return m?<span style={{fontWeight:700,color:m.price>=(inventoryState[p.id]?.avgCost||0)?C.green:C.danger}}>{fmtNum(m.price,4)}</span>:<span style={{color:C.muted,fontSize:11}}>—</span>;}},
        {label:"G/P no real.",r:true,fn:p=>{const s=inventoryState[p.id]||{};const m=latestMkt(p.id);if(!m||!s.qty) return<span style={{color:C.muted}}>—</span>;const gp=(m.price-s.avgCost)*s.qty;return<span style={{fontWeight:700,color:gp>=0?C.green:C.danger}}>{gp>=0?"+":""}{fmtNum(gp,2)}</span>;}},
        {label:"",fn:p=><button style={S.bsm(C.navy,C.gold)} onClick={()=>{setSel(p);setView("detail");}}>Ver</button>},
      ]} rows={products}/>
    </div>}
  </div>;
}



// ════════════════════════════════════════════════════════════════
//  FIXED ASSETS SECTION
// ════════════════════════════════════════════════════════════════
function FixedAssetsSection({rates,userId,entityId}){
  const [assets,setAssets]=useState(()=>lsLoad("ac_fa",[]));
  const [mkt,setMkt]=useState(()=>lsLoad("ac_fa_mkt",{}));
  const [view,setView]=useState("list"); const [sel,setSel]=useState(null); const [msg,setMsg]=useState(null); const [err,setErr]=useState("");

  useEffect(()=>{
    if(!userId) return;
    dbLoad("ac_fixed_assets", userId, entityId, "ac_fa",[]).then(setAssets);
    dbLoad("ac_fixed_assets_market", userId, entityId, "ac_fa_mkt_rows",[]).then(rows=>{
      const map={};
      rows.forEach(r=>{ if(!map[r.assetId]) map[r.assetId]=[]; map[r.assetId].push(r); });
      Object.keys(map).forEach(k=>map[k].sort((a,b)=>b.date.localeCompare(a.date)));
      setMkt(map); lsSave("ac_fa_mkt",map);
    });
  },[userId]);

  const eA={name:"",code:"",category:"",currency:"CLP",acquisitionCost:"",acquisitionDate:today(),usefulLife:"",residualValue:"",depreciationMethod:"lineal",notes:""};
  const eMk={assetId:"",date:today(),price:"",source:""};
  const [aF,setAF]=useState(eA); const [mkF,setMkF]=useState(eMk);

  function computeDep(asset){
    const acq=new Date(asset.acquisitionDate+"T12:00:00"),to=new Date();
    const mo=Math.max(0,(to.getFullYear()-acq.getFullYear())*12+(to.getMonth()-acq.getMonth()));
    const life=asset.usefulLife||1,base=(asset.acquisitionCost||0)-(asset.residualValue||0);
    const monthly=base/(life*12),totalDep=Math.min(base,monthly*mo);
    return{totalDep,bookValue:(asset.acquisitionCost||0)-totalDep,monthlyDep:monthly,pctDep:Math.min(100,base>0?totalDep/base*100:0)};
  }
  const latestMkt=id=>(mkt[id]||[])[0]||null;
  const totalBVCLP=useMemo(()=>assets.reduce((s,a)=>s+toCLP(computeDep(a).bookValue,a.currency,rates),0),[assets,rates]);

  function saveAsset(){
    setErr(""); if(!aF.name.trim()) return setErr("Nombre obligatorio.");
    const cost=parseFloat(aF.acquisitionCost); if(!cost||cost<=0) return setErr("Costo inválido.");
    const life=parseInt(aF.usefulLife); if(!life||life<1) return setErr("Vida útil inválida.");
    const a={id:sel?.id||genId(),...aF,acquisitionCost:cost,usefulLife:life,residualValue:parseFloat(aF.residualValue)||0,createdAt:sel?.createdAt||new Date().toISOString()};
    const u=assets.find(x=>x.id===a.id)?assets.map(x=>x.id===a.id?a:x):[...assets,a];
    setAssets(u); dbUpsert("ac_fixed_assets", userId, entityId, a,"ac_fa",u);
    setMsg({ok:true,text:`"${a.name}" guardado.`}); setView("list");setSel(null);setAF(eA);setTimeout(()=>setMsg(null),3000);
  }
  function saveMk(){
    setErr(""); if(!mkF.assetId) return setErr("Selecciona activo.");
    const price=parseFloat(mkF.price); if(isNaN(price)||price<0) return setErr("Precio inválido.");
    const entry={id:genId(),assetId:mkF.assetId,date:mkF.date,price,source:mkF.source,createdAt:new Date().toISOString()};
    const newMkt={...mkt,[mkF.assetId]:[...(mkt[mkF.assetId]||[]),entry].sort((a,b)=>b.date.localeCompare(a.date))};
    setMkt(newMkt); lsSave("ac_fa_mkt",newMkt); dbUpsert("ac_fixed_assets_market", userId, entityId, entry,"ac_fa_mkt_rows",[...Object.values(newMkt).flat()]);
    setMsg({ok:true,text:"Precio registrado."}); setView("list");setMkF(eMk);setTimeout(()=>setMsg(null),3000);
  }
  function delAsset(id){
    if(!confirm("¿Eliminar activo?")) return;
    const u=assets.filter(a=>a.id!==id); setAssets(u); dbDelete("ac_fixed_assets", userId, id, "ac_fa", entityId, u);
    if(sel?.id===id){setSel(null);setView("list");}
  }
  if(view==="new"||view==="edit") return <div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>{view==="edit"?"Editar":"Nuevo"} Activo Fijo</span></div><div style={S.cBody}>
    {err&&<Msg>{err}</Msg>}
    <div style={{...S.g3,marginBottom:14}}><Inp label="Nombre *" placeholder="Camión Volvo FH16" value={aF.name} onChange={e=>setAF(f=>({...f,name:e.target.value}))}/><Inp label="Código" placeholder="AF-001" value={aF.code} onChange={e=>setAF(f=>({...f,code:e.target.value}))}/><Inp label="Categoría" placeholder="Vehículo, Maquinaria…" value={aF.category} onChange={e=>setAF(f=>({...f,category:e.target.value}))}/></div>
    <div style={{...S.g4,marginBottom:14}}><Sel label="Moneda" options={CURRENCIES} value={aF.currency} onChange={e=>setAF(f=>({...f,currency:e.target.value}))}/><Inp label="Costo adquisición *" type="number" min="0" value={aF.acquisitionCost} onChange={e=>setAF(f=>({...f,acquisitionCost:e.target.value}))}/><Inp label="Valor residual" type="number" min="0" value={aF.residualValue} onChange={e=>setAF(f=>({...f,residualValue:e.target.value}))}/><Inp label="Fecha adquisición" type="date" value={aF.acquisitionDate} onChange={e=>setAF(f=>({...f,acquisitionDate:e.target.value}))}/></div>
    <div style={{...S.g2,marginBottom:14}}><Inp label="Vida útil (años) *" type="number" min="1" value={aF.usefulLife} onChange={e=>setAF(f=>({...f,usefulLife:e.target.value}))}/><Sel label="Método" options={[{v:"lineal",l:"Lineal (cuota fija)"}]} value={aF.depreciationMethod} onChange={e=>setAF(f=>({...f,depreciationMethod:e.target.value}))}/></div>
    <Field label="Notas"><textarea style={S.textarea} value={aF.notes} onChange={e=>setAF(f=>({...f,notes:e.target.value}))}/></Field>
    <div style={{display:"flex",gap:10,marginTop:14}}><Btn onClick={saveAsset}>Guardar</Btn><Btn v="outline" onClick={()=>{setView("list");setSel(null);setAF(eA);}}>Cancelar</Btn></div>
  </div></div>;


    if(view==="market") return <div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>Precio de Mercado — Activo Fijo</span></div><div style={S.cBody}>
    {err&&<Msg>{err}</Msg>}
    <div style={{...S.g4,marginBottom:14}}>
      <Field label="Activo *"><select style={S.select} value={mkF.assetId} onChange={e=>setMkF(f=>({...f,assetId:e.target.value}))}><option value="">Seleccionar…</option>{assets.map(a=><option key={a.id} value={a.id}>{a.name}</option>)}</select></Field>
      <Inp label="Fecha *" type="date" value={mkF.date} onChange={e=>setMkF(f=>({...f,date:e.target.value}))}/>
      <Inp label="Precio mercado *" type="number" min="0" step="any" value={mkF.price} onChange={e=>setMkF(f=>({...f,price:e.target.value}))}/>
      <Inp label="Fuente" placeholder="Tasación, mercado…" value={mkF.source} onChange={e=>setMkF(f=>({...f,source:e.target.value}))}/>
    </div>
    <div style={{display:"flex",gap:10,marginTop:14}}><Btn onClick={saveMk}>Guardar</Btn><Btn v="outline" onClick={()=>setView("list")}>Cancelar</Btn></div>
  </div></div>;

  return <div>
    {msg&&<Msg ok={msg.ok}>{msg.text}</Msg>}
    <StatGrid stats={[{label:"Activos fijos",value:assets.length},{label:"Valor libro total (CLP)",value:fmtCLP(totalBVCLP)},{label:"Con precio mercado",value:Object.keys(mkt).filter(k=>(mkt[k]||[]).length>0).length},{label:"Categorías",value:[...new Set(assets.map(a=>a.category).filter(Boolean))].length||0}]}/>
    <div style={{display:"flex",gap:10,justifyContent:"flex-end",marginBottom:14}}>
      <Btn v="gold" onClick={()=>{setMkF({assetId:"",date:today(),price:"",source:""});setView("market");}}>📈 Precio mercado</Btn>
      <Btn onClick={()=>{setAF(eA);setSel(null);setView("new");}}>+ Nuevo activo</Btn>
    </div>
    {assets.length===0?<div style={{...S.card,...S.empty}}><div style={{fontSize:32,marginBottom:10}}>🏭</div><div style={{fontFamily:"'Georgia',serif",color:C.muted,marginBottom:14}}>Sin activos fijos</div><Btn onClick={()=>setView("new")}>Registrar primer activo</Btn></div>
    :<div style={S.card}><div style={S.cHead()}><span style={S.cTitle}>Registro de Activos Fijos</span></div>
      <DataTable cols={[
        {label:"Código",fn:a=><code style={{fontSize:11,background:"#f1f5f9",padding:"2px 6px",borderRadius:3}}>{a.code||"—"}</code>},
        {label:"Nombre",fn:a=><b style={{fontFamily:"'Georgia',serif"}}>{a.name}</b>},
        {label:"Categoría",fn:a=><span style={{fontSize:12,color:C.muted}}>{a.category||"—"}</span>},
        {label:"Mon.",key:"currency"},
        {label:"Costo",r:true,fn:a=><span>{fmtNum(a.acquisitionCost,2)}</span>},
        {label:"Dep. acum.",r:true,fn:a=><span style={{color:C.danger}}>{fmtNum(computeDep(a,null).totalDep,2)}</span>},
        {label:"Valor libro",r:true,fn:a=><b style={{fontFamily:"'Georgia',serif"}}>{fmtNum(computeDep(a,null).bookValue,2)}</b>},
        {label:"% Dep.",fn:a=>{const d=computeDep(a,null);return<div style={{display:"flex",alignItems:"center",gap:6}}><div style={{width:48,height:5,background:"#e2e8f0",borderRadius:3}}><div style={{width:`${d.pctDep}%`,height:5,background:d.pctDep>=100?C.muted:C.gold,borderRadius:3}}/></div><span style={{fontSize:10.5}}>{d.pctDep.toFixed(0)}%</span></div>;}},
        {label:"Precio mkt.",r:true,fn:a=>{const m=latestMkt(a.id);const bv=computeDep(a,null).bookValue;return m?<span style={{fontWeight:700,color:m.price>=bv?C.green:C.danger}}>{fmtNum(m.price,2)}</span>:<span style={{color:C.muted,fontSize:11}}>—</span>;}},
        {label:"",fn:a=><div style={{display:"flex",gap:6}}><button style={S.bsm(C.navy,C.gold)} onClick={()=>{setSel(a);setAF({...a,acquisitionCost:String(a.acquisitionCost),usefulLife:String(a.usefulLife),residualValue:String(a.residualValue)});setView("edit");}}>Editar</button><button style={S.bsm("transparent",C.danger)} onClick={()=>delAsset(a.id)}>✕</button></div>},
      ]} rows={assets}/>
    </div>}
  </div>;
}



// ════════════════════════════════════════════════════════════════
//  ENTITY MANAGER
// ════════════════════════════════════════════════════════════════
function EntityManager({entities,setEntities,userId,onClose}){
  const [view,setView]=useState("list");
  const [f,setF]=useState({rut:"",name:"",giro:""});
  const [sel,setSel]=useState(null);
  const [err,setErr]=useState("");

  const formatRut=v=>{
    const clean=v.replace(/[^0-9kK]/g,"");
    if(clean.length<2) return clean;
    const body=clean.slice(0,-1), dv=clean.slice(-1).toUpperCase();
    return body.replace(/\B(?=(\d{3})+(?!\d))/g,".")+"-"+dv;
  };

  function save(){
    setErr("");
    if(!f.rut.trim()) return setErr("RUT obligatorio.");
    if(!f.name.trim()) return setErr("Razón social obligatoria.");
    const entity={id:sel?.id||genId(),rut:f.rut.trim(),name:f.name.trim(),giro:f.giro.trim(),createdAt:sel?.createdAt||new Date().toISOString()};
    const u=entities.find(e=>e.id===entity.id)?entities.map(e=>e.id===entity.id?entity:e):[...entities,entity];
    setEntities(u); dbUpsertEntity(userId,entity,u);
    setView("list"); setSel(null); setF({rut:"",name:"",giro:""});
  }
  function del(id){
    if(!confirm("¿Eliminar empresa? Se eliminarán todos sus datos.")) return;
    const u=entities.filter(e=>e.id!==id); setEntities(u); dbDeleteEntity(userId,id,u);
  }

  return <div style={{position:"fixed",inset:0,background:"rgba(0,0,0,.6)",display:"flex",alignItems:"center",justifyContent:"center",zIndex:1000}}>
    <div style={{background:"#fff",borderRadius:6,width:580,maxHeight:"80vh",overflowY:"auto",boxShadow:"0 20px 60px rgba(0,0,0,.3)"}}>
      <div style={{...S.cHead(),borderRadius:"6px 6px 0 0"}}>
        <span style={S.cTitle}>🏢 Gestión de Empresas</span>
        <div style={{display:"flex",gap:8}}>
          {view==="list"&&<Btn sm onClick={()=>{setF({rut:"",name:"",giro:""});setSel(null);setView("new");}}>+ Nueva empresa</Btn>}
          <Btn sm v="outline" onClick={onClose}>✕ Cerrar</Btn>
        </div>
      </div>
      <div style={S.cBody}>
        {view==="list"&&<>
          {entities.length===0&&<div style={{...S.empty,padding:30}}><div style={{fontSize:32}}>🏢</div><div style={{color:C.muted,marginTop:8}}>Sin empresas. Crea la primera.</div></div>}
          {entities.map(e=><div key={e.id} style={{display:"flex",alignItems:"center",justifyContent:"space-between",padding:"12px 0",borderBottom:`1px solid ${C.border}`}}>
            <div>
              <div style={{fontFamily:"'Georgia',serif",fontWeight:700,fontSize:14}}>{e.name}</div>
              <div style={{fontSize:11,color:C.muted,marginTop:2}}>RUT: {e.rut}{e.giro?` · ${e.giro}`:""}</div>
            </div>
            <div style={{display:"flex",gap:8}}>
              <Btn sm v="outline" onClick={()=>{setSel(e);setF({rut:e.rut,name:e.name,giro:e.giro||""});setView("edit");}}>Editar</Btn>
              <Btn sm v="danger" onClick={()=>del(e.id)}>✕</Btn>
            </div>
          </div>)}
        </>}
        {(view==="new"||view==="edit")&&<>
          {err&&<Msg>{err}</Msg>}
          <div style={{...S.g2,marginBottom:14}}>
            <div>
              <label style={S.label}>RUT *</label>
              <input style={S.input} placeholder="76.123.456-7" value={f.rut}
                onChange={e=>setF(p=>({...p,rut:formatRut(e.target.value)}))}/>
            </div>
            <Inp label="Razón Social *" placeholder="Viento Sur SpA" value={f.name} onChange={e=>setF(p=>({...p,name:e.target.value}))}/>
          </div>
          <Inp label="Giro" placeholder="Inversiones y rentas" value={f.giro} onChange={e=>setF(p=>({...p,giro:e.target.value}))}/>
          <div style={{display:"flex",gap:10,marginTop:18}}>
            <Btn onClick={save}>{view==="edit"?"Guardar cambios":"Crear empresa"}</Btn>
            <Btn v="outline" onClick={()=>{setView("list");setSel(null);}}>Cancelar</Btn>
          </div>
        </>}
      </div>
    </div>
  </div>;
}

// ════════════════════════════════════════════════════════════════
//  MAIN APP
// ════════════════════════════════════════════════════════════════

// ════════════════════════════════════════════════════════════════
//  CIERRE MENSUAL — Reajuste de Activos por IPC
// ════════════════════════════════════════════════════════════════
function CierreMensualSection({entries, setEntries, userId, entityId, accounts}){
  const [selMonth, setSelMonth]   = useState(()=>{
    const d=new Date(); d.setDate(1); d.setMonth(d.getMonth()-1);
    return d.toISOString().slice(0,7);
  });
  const [ipc, setIpc]             = useState(null);   // {valor, fecha} from API
  const [ipcLoading, setIpcLoading] = useState(false);
  const [ipcError, setIpcError]   = useState(null);
  const [preview, setPreview]     = useState(null);   // [{counterparty, saldo, reajuste}]
  const [generating, setGenerating] = useState(false);
  const [msg, setMsg]             = useState(null);
  const [ipcManual, setIpcManual] = useState("");  // fallback manual input

  // All months that have entries
  const allMonths = useMemo(()=>[...new Set(entries.map(e=>e.date.slice(0,7)))].sort(),[entries]);

  // Fetch IPC for selected month from mindicador.cl
  async function fetchIPC(month){
    setIpcLoading(true); setIpcError(null); setIpc(null); setPreview(null);
    try {
      const [year] = month.split("-");
      const res = await fetch(`https://mindicador.cl/api/ipc/${year}`);
      if(!res.ok) throw new Error("HTTP "+res.status);
      const data = await res.json();
      // serie: [{fecha, valor}] — find the entry whose fecha matches our month
      const targetMonth = month; // "YYYY-MM"
      const entry = (data.serie||[]).find(s=>s.fecha?.slice(0,7)===targetMonth);
      if(!entry) throw new Error(`Sin dato IPC para ${month}`);
      setIpc({valor:entry.valor, fecha:entry.fecha?.slice(0,10)});
    } catch(e){
      setIpcError(e.message||"Error al obtener IPC");
      setIpcManual(""); // reset manual on new error
    } finally {
      setIpcLoading(false);
    }
  }

  useEffect(()=>{ if(selMonth) fetchIPC(selMonth); },[selMonth]);

  // Compute saldo of account 1630 per counterparty AT START of selected month
  // = all entries BEFORE selMonth
  const balancesByCP = useMemo(()=>{
    const b={};
    entries.forEach(e=>{
      if(e.date.slice(0,7) >= selMonth) return; // only entries before this month
      e.rows.forEach(r=>{
        if(r.account !== "1630") return;
        const cp = (r.counterparty||"(sin contraparte)").trim();
        if(!b[cp]) b[cp]={debit:0,credit:0};
        b[cp].debit  += r.debit||0;
        b[cp].credit += r.credit||0;
      });
    });
    // Net saldo for each CP (debit - credit for asset account)
    return Object.entries(b)
      .map(([cp,v])=>({counterparty:cp, saldo:v.debit-v.credit}))
      .filter(v=>Math.abs(v.saldo)>0.5)
      .sort((a,b)=>a.counterparty.localeCompare(b.counterparty));
  },[entries,selMonth]);

  // Generate preview table
  function computePreview(){
    if(!ipc||balancesByCP.length===0) return;
    const varIPC = ipc.valor / 100; // e.g. 0.4 % → 0.004
    const rows = balancesByCP.map(({counterparty,saldo})=>({
      counterparty,
      saldo: Math.round(saldo),
      reajuste: Math.round(saldo * varIPC),
    }));
    setPreview(rows);
  }

  // Check if reajuste already exists for this month
  const alreadyGenerated = useMemo(()=>
    entries.some(e=>e.date.startsWith(selMonth) && e.reference==="Auto-Reajuste-IPC")
  ,[entries, selMonth]);

  async function generateEntries(){
    if(!preview||preview.length===0) return;
    setGenerating(true);
    try {
      const varIPC = ipc.valor/100;
      const lastDay = new Date(selMonth+"-01");
      lastDay.setMonth(lastDay.getMonth()+1); lastDay.setDate(0);
      const date = lastDay.toISOString().slice(0,10);

      let curEntries = [...entries];
      for(const row of preview){
        if(row.reajuste===0) continue;
        const isPositive = row.reajuste > 0;
        const abs = Math.abs(row.reajuste);
        const entryRows = isPositive
          ? [ // activo sube, ingreso
              {id:genId(),account:"1630",debit:abs,credit:0,counterparty:row.counterparty,note:"Reajuste IPC"},
              {id:genId(),account:"4300",debit:0,credit:abs,counterparty:row.counterparty,note:"Reajuste IPC"},
            ]
          : [ // activo baja, pérdida
              {id:genId(),account:"5300",debit:abs,credit:0,counterparty:row.counterparty,note:"Reajuste IPC negativo"},
              {id:genId(),account:"1630",debit:0,credit:abs,counterparty:row.counterparty,note:"Reajuste IPC negativo"},
            ];
        const n = curEntries.length+1;
        const entry = {
          id:genId(), number:n, date,
          description:`Reajuste IPC ${selMonth} — ${row.counterparty} (${ipc.valor>0?"+":""}${ipc.valor}%)`,
          reference:"Auto-Reajuste-IPC",
          rows:entryRows, totalDebit:abs, totalCredit:abs,
          createdAt:new Date().toISOString(),
        };
        curEntries = [...curEntries, entry];
        await dbUpsertEntry(userId, entityId, entry, curEntries);
      }
      setEntries(curEntries);
      setMsg({ok:true, text:`✓ ${preview.filter(r=>r.reajuste!==0).length} asientos de reajuste generados para ${selMonth}.`});
      setPreview(null);
      setTimeout(()=>setMsg(null),6000);
    } catch(e){
      setMsg({ok:false, text:"Error: "+e.message});
    } finally {
      setGenerating(false);
    }
  }

  const fmtPct = v => v==null?"—":`${v>0?"+":""}${v}%`;
  const totalReajuste = preview ? preview.reduce((s,r)=>s+r.reajuste,0) : 0;

  return <div>
    {msg&&<Msg ok={msg.ok}>{msg.text}</Msg>}

    {/* ── Header ── */}
    <StatGrid stats={[
      {label:"Mes seleccionado",  value:selMonth?new Date(selMonth+"-15").toLocaleDateString("es-CL",{month:"long",year:"numeric"}):"—"},
      {label:"IPC del mes",       value:ipcLoading?"…":!ipc?"Sin dato":fmtPct(ipc.valor)+(ipc.manual?" (manual)":"")},
      {label:"Contrapartes 1630", value:balancesByCP.length},
      {label:"Saldo total 1630",  value:fmtCLP(balancesByCP.reduce((s,v)=>s+v.saldo,0))},
    ]}/>

    {/* ── Controls ── */}
    <div style={S.card}><div style={S.cBody}>
      <div style={{display:"flex",gap:14,flexWrap:"wrap",alignItems:"flex-end"}}>
        <div style={{flex:"0 0 200px"}}>
          <label style={S.label}>Mes de cierre</label>
          <select style={S.select} value={selMonth} onChange={e=>setSelMonth(e.target.value)}>
            {allMonths.map(m=><option key={m} value={m}>
              {new Date(m+"-15").toLocaleDateString("es-CL",{month:"long",year:"numeric"})}
            </option>)}
          </select>
        </div>

        {/* IPC status */}
        <div style={{flex:"0 0 240px"}}>
          <label style={S.label}>
            IPC del mes&nbsp;
            <a href="https://www.ine.gob.cl/estadisticas/macro/inflacion/ipc" target="_blank" rel="noreferrer"
              style={{fontSize:9,color:C.gold,fontWeight:400,textDecoration:"none"}}>
              ver INE ↗
            </a>
          </label>
          {/* Loaded OK */}
          {ipc&&!ipcLoading&&!ipcError&&(
            <div style={{...S.input,background:"#f0fdf4",display:"flex",alignItems:"center",gap:8,cursor:"default"}}>
              <span style={{fontWeight:700,fontSize:15,color:ipc.valor>0?C.green:ipc.valor<0?C.danger:C.muted}}>{fmtPct(ipc.valor)}</span>
              <span style={{fontSize:10,color:C.muted}}>mindicador.cl · {fmtDate(ipc.fecha)}</span>
              <button onClick={()=>{setIpc(null);setIpcError("Ingresado manualmente");}}
                style={{marginLeft:"auto",background:"none",border:"none",color:C.muted,fontSize:10,cursor:"pointer"}} title="Editar manualmente">✎</button>
            </div>
          )}
          {/* Loading */}
          {ipcLoading&&(
            <div style={{...S.input,background:"#f8f6f1",color:C.muted,fontSize:12}}>Consultando mindicador.cl…</div>
          )}
          {/* Error or manual mode */}
          {!ipc&&!ipcLoading&&(
            <div>
              {ipcError&&<div style={{fontSize:10.5,color:C.danger,marginBottom:5}}>⚠ {ipcError} — ingresa manualmente:</div>}
              <div style={{display:"flex",gap:6,alignItems:"center"}}>
                <input
                  style={{...S.input,width:90,textAlign:"right"}}
                  type="number" step="0.01" placeholder="ej: 0.4"
                  value={ipcManual}
                  onChange={e=>setIpcManual(e.target.value)}
                />
                <span style={{fontSize:12,color:C.muted}}>%</span>
                <Btn sm onClick={()=>{
                  const v=parseFloat(ipcManual);
                  if(isNaN(v)) return;
                  setIpc({valor:v, fecha:selMonth+"-01", manual:true});
                  setIpcError(null);
                }} disabled={!ipcManual}>Aplicar</Btn>
              </div>
              <div style={{fontSize:9.5,color:C.muted,marginTop:4}}>
                Consulta el valor en <a href="https://www.ine.gob.cl/estadisticas/macro/inflacion/ipc" target="_blank" rel="noreferrer" style={{color:C.gold}}>ine.gob.cl ↗</a>
              </div>
            </div>
          )}
        </div>

        <div style={{display:"flex",gap:8,alignItems:"flex-end"}}>
          <Btn onClick={computePreview} disabled={!ipc||ipcLoading||balancesByCP.length===0}>
            Vista previa
          </Btn>
          {!ipc&&!ipcLoading&&<Btn v="outline" onClick={()=>fetchIPC(selMonth)}>↻ Reintentar API</Btn>}
        </div>
      </div>

      {alreadyGenerated&&<div style={{marginTop:12,background:"#fef9c3",border:"1px solid #f59e0b",borderRadius:3,padding:"8px 14px",fontSize:12,color:"#92400e"}}>
        ⚠ Ya existen asientos de reajuste generados para {selMonth}. Si generas de nuevo se duplicarán.
      </div>}
    </div></div>

    {/* ── Saldos por contraparte ── */}
    {balancesByCP.length>0&&<div style={S.card}>
      <div style={S.cHead()}><span style={S.cTitle}>Saldos cuenta 1630 — inicio de {selMonth}</span></div>
      <div style={{overflowX:"auto"}}>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:12.5}}>
          <thead><tr>
            {["Contraparte","Saldo inicio mes","IPC","Reajuste estimado"].map((h,i)=>(
              <th key={i} style={{...S.th,textAlign:i>=1?"right":"left"}}>{h}</th>
            ))}
          </tr></thead>
          <tbody>{balancesByCP.map((v,i)=>{
            const rej = ipc ? Math.round(v.saldo*(ipc.valor/100)) : null;
            return <tr key={v.counterparty} style={{background:i%2===0?"#fafaf9":"#fff"}}>
              <td style={{...S.td,fontFamily:"'Georgia',serif"}}>{v.counterparty}</td>
              <td style={{...S.td,textAlign:"right",fontWeight:700}}>{fmtCLP(v.saldo)}</td>
              <td style={{...S.td,textAlign:"right",color:ipc?.valor>0?C.green:ipc?.valor<0?C.danger:C.muted}}>
                {ipcLoading?"…":fmtPct(ipc?.valor)}
              </td>
              <td style={{...S.td,textAlign:"right",fontWeight:700,
                color:rej==null?C.muted:rej>=0?C.green:C.danger}}>
                {rej==null?"—":`${rej>=0?"+":""}${fmtCLP(Math.abs(rej))}`}
              </td>
            </tr>;
          })}</tbody>
        </table>
      </div>
    </div>}

    {/* ── Preview asientos ── */}
    {preview&&<div style={S.card}>
      <div style={S.cHead()}>
        <span style={S.cTitle}>Asientos a generar</span>
        <span style={{fontSize:11,color:C.gold}}>{preview.filter(r=>r.reajuste!==0).length} asientos</span>
      </div>
      <div style={{overflowX:"auto"}}>
        <table style={{width:"100%",borderCollapse:"collapse",fontSize:12.5}}>
          <thead><tr>
            {["Contraparte","Cuenta Débito","Cuenta Crédito","Monto reajuste"].map((h,i)=>(
              <th key={i} style={{...S.th,textAlign:i>=3?"right":"left"}}>{h}</th>
            ))}
          </tr></thead>
          <tbody>{preview.map((r,i)=>{
            if(r.reajuste===0) return null;
            const isPos = r.reajuste>0;
            return <tr key={r.counterparty} style={{background:i%2===0?"#fafaf9":"#fff"}}>
              <td style={{...S.td,fontFamily:"'Georgia',serif"}}>{r.counterparty}</td>
              <td style={S.td}><code style={{fontSize:11,color:C.muted}}>{isPos?"1630":"5300"}</code> {isPos?"Inversiones (sube)":"Pérdida en Inversiones"}</td>
              <td style={S.td}><code style={{fontSize:11,color:C.muted}}>{isPos?"4300":"1630"}</code> {isPos?"Ganancia en Inversiones":"Inversiones (baja)"}</td>
              <td style={{...S.td,textAlign:"right",fontWeight:700,color:isPos?C.green:C.danger}}>
                {isPos?"+":"-"}{fmtCLP(Math.abs(r.reajuste))}
              </td>
            </tr>;
          })}</tbody>
          <tfoot><tr style={{background:C.navy}}>
            <td colSpan={3} style={{...S.td,color:C.gold,fontWeight:700,fontSize:10,letterSpacing:2,textTransform:"uppercase"}}>Total reajuste</td>
            <td style={{...S.td,textAlign:"right",fontWeight:700,color:totalReajuste>=0?"#86efac":"#fca5a5"}}>
              {totalReajuste>=0?"+":""}{fmtCLP(totalReajuste)}
            </td>
          </tr></tfoot>
        </table>
      </div>
      <div style={{padding:"14px 16px",display:"flex",gap:10,justifyContent:"flex-end",borderTop:`1px solid ${C.border}`}}>
        <Btn v="outline" onClick={()=>setPreview(null)}>Cancelar</Btn>
        <Btn onClick={generateEntries} disabled={generating}>
          {generating?"Generando…":"✓ Generar asientos de reajuste"}
        </Btn>
      </div>
    </div>}

    {/* ── Empty state ── */}
    {balancesByCP.length===0&&<div style={{...S.card,...S.empty}}>
      <div style={{fontSize:32,marginBottom:10}}>📅</div>
      <div style={{fontFamily:"'Georgia',serif",color:C.muted}}>
        Sin saldo en cuenta 1630 antes de {selMonth}
      </div>
    </div>}
  </div>;
}

const SECTIONS=[
  {id:"accounting",label:"Contabilidad",icon:"⚖",tabs:[{id:"new",label:"Nuevo asiento"},{id:"entries",label:"Asientos"},{id:"reports",label:"Reportes"},{id:"accounts",label:"Plan de cuentas"}]},
  {id:"liabilities",label:"Pasivos",icon:"🏦",tabs:[]},
  {id:"investments",label:"Inversiones",icon:"📊",tabs:[]},
  {id:"inventory",label:"Inventario",icon:"📦",tabs:[]},
  {id:"fixed",label:"Activos Fijos",icon:"🏭",tabs:[]},
  {id:"cierre",label:"Cierre Mensual",icon:"📅",tabs:[]},
];

export default function App(){
  const {user,loading,signInGoogle,signInGitHub,signOut}=useAuth();
  const [section,setSection]=useState("accounting");
  const [accTab,setAccTab]=useState("new");
  const {rates,meta,fetchRates}=useFxRates();
  const [showEntityMgr,setShowEntityMgr]=useState(false);

  const [entities,setEntities]=useState(()=>lsLoad("ac_entities",[]));
  const [entityId,setEntityId]=useState(()=>lsLoad("ac_current_entity",null));
  const entity=entities.find(e=>e.id===entityId)||null;

  const [accounts,setAccounts]=useState(()=>lsLoad("ac_accounts"+(entityId?":"+entityId:""),DEFAULT_ACCOUNTS));
  const [entries, setEntries] =useState(()=>lsLoad("ac_entries"+(entityId?":"+entityId:""),[]));

  useEffect(()=>{
    if(!user) return;
    dbLoadEntities(user.id).then(ents=>{
      setEntities(ents);
      const saved=lsLoad("ac_current_entity",null);
      if(!saved && ents.length>0){ setEntityId(ents[0].id); lsSave("ac_current_entity",ents[0].id); }
    });
  },[user]);

  useEffect(()=>{
    if(!user) return;
    lsSave("ac_current_entity",entityId);
    dbLoadAccounts(user.id, entityId).then(setAccounts);
    dbLoadEntries(user.id, entityId).then(setEntries);
  },[user, entityId]);

  function selectEntity(id){ setEntityId(id); setSection("accounting"); setAccTab("new"); }

  if(loading) return(
    <div style={{background:"#0f172a",minHeight:"100vh",display:"flex",alignItems:"center",justifyContent:"center"}}>
      <div style={{color:"#b8973f",fontFamily:"'Georgia',serif",fontSize:18}}>Cargando…</div>
    </div>
  );

  if(!user) return <LoginScreen signInGoogle={signInGoogle} signInGitHub={signInGitHub}/>;

  const uid=user.id;
  const currentSection=SECTIONS.find(s=>s.id===section);

  return(
    <div style={S.app}>
      {showEntityMgr&&<EntityManager entities={entities} setEntities={setEntities} userId={uid} onClose={()=>setShowEntityMgr(false)}/>}
      <header style={S.topBar}>
        <div style={{display:"flex",alignItems:"center",gap:16}}>
          <div>
            <div style={S.logo}>⚖ LibroDiario</div>
            <div style={S.logosub}>Sistema Contable Integrado</div>
          </div>
          <div style={{borderLeft:"1px solid #334155",paddingLeft:16,display:"flex",alignItems:"center",gap:8}}>
            {entities.length===0
              ? <button onClick={()=>setShowEntityMgr(true)} style={{...S.bsm(C.gold,"#0f172a"),fontSize:11,padding:"4px 10px"}}>+ Crear empresa</button>
              : <select value={entityId||""} onChange={e=>selectEntity(e.target.value)}
                  style={{background:"#1e293b",border:`1px solid ${C.gold}`,borderRadius:3,color:"#fff",padding:"4px 8px",fontSize:12,fontFamily:"'Georgia',serif",cursor:"pointer",maxWidth:200}}>
                  <option value="">— Sin empresa —</option>
                  {entities.map(e=><option key={e.id} value={e.id}>{e.name}</option>)}
                </select>
            }
            <button onClick={()=>setShowEntityMgr(true)} style={{background:"none",border:"none",color:C.muted,cursor:"pointer",fontSize:13,padding:"2px 4px"}} title="Gestionar empresas">⚙</button>
          </div>
        </div>
        <nav style={{display:"flex",alignItems:"center"}}>
          {SECTIONS.map(s=>(
            <button key={s.id} style={{background:"none",border:"none",borderBottom:section===s.id?`3px solid ${C.gold}`:"3px solid transparent",color:section===s.id?"#fff":C.muted,padding:"0 16px",height:60,cursor:"pointer",fontFamily:"'Georgia',serif",fontSize:12,fontWeight:section===s.id?700:400,letterSpacing:1,textTransform:"uppercase",whiteSpace:"nowrap",transition:"all .15s"}} onClick={()=>setSection(s.id)}>
              {s.icon} {s.label}
            </button>
          ))}
          <div style={{display:"flex",alignItems:"center",gap:10,marginLeft:20,paddingLeft:20,borderLeft:"1px solid #334155"}}>
            {entity&&<div style={{textAlign:"right"}}>
              <div style={{fontSize:10,color:C.gold,fontWeight:700}}>{entity.name}</div>
              <div style={{fontSize:9,color:C.muted}}>RUT {entity.rut}</div>
            </div>}
            <span style={{fontSize:11,color:"#94a3b8",maxWidth:120,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{user.email||user.user_metadata?.full_name||"Usuario"}</span>
            <button onClick={signOut} style={{...S.bsm("transparent","#94a3b8"),border:"1px solid #334155",fontSize:10}}>Salir</button>
          </div>
        </nav>
      </header>
      {!entity&&entities.length>0&&<div style={{background:"#7c3aed",color:"#fff",textAlign:"center",padding:"8px",fontSize:12}}>⚠ Selecciona una empresa en el selector superior para ver y registrar datos.</div>}
      {!entity&&entities.length===0&&<div style={{background:C.gold,color:"#0f172a",textAlign:"center",padding:"8px",fontSize:12,fontWeight:700}}>👆 Crea tu primera empresa haciendo clic en "+ Crear empresa" para comenzar.</div>}
      <FxBar rates={rates} meta={meta} fetchRates={fetchRates}/>
      {section==="accounting"&&(
        <div style={S.subBar}>
          {currentSection.tabs.map(t=><button key={t.id} style={S.subTab(accTab===t.id)} onClick={()=>setAccTab(t.id)}>{t.label}</button>)}
        </div>
      )}
      <main style={S.body}>
        {section==="accounting"&&accTab==="new"      &&<NewEntryTab  accounts={accounts} entries={entries} setEntries={setEntries} userId={uid} entityId={entityId}/>}
        {section==="accounting"&&accTab==="entries"  &&<EntriesTab   accounts={accounts} entries={entries} setEntries={setEntries} userId={uid} entityId={entityId}/>}
        {section==="accounting"&&accTab==="reports"  &&<ReportsTab   accounts={accounts} entries={entries}/>}
        {section==="accounting"&&accTab==="accounts" &&<AccountsTab  accounts={accounts} setAccounts={setAccounts} userId={uid} entityId={entityId}/>}
        {section==="liabilities"&&<LiabilitiesSection rates={rates} userId={uid} entityId={entityId} accounts={accounts} entries={entries} setEntries={setEntries}/>}
        {section==="investments"&&<InvestmentsSection  rates={rates} userId={uid} entityId={entityId} accounts={accounts} entries={entries} setEntries={setEntries}/>}
        {section==="inventory"  &&<InventorySection    rates={rates} userId={uid} entityId={entityId} accounts={accounts} entries={entries} setEntries={setEntries}/>}
        {section==="fixed"      &&<FixedAssetsSection  rates={rates} userId={uid} entityId={entityId}/>}
        {section==="cierre"     &&<CierreMensualSection entries={entries} setEntries={setEntries} userId={uid} entityId={entityId} accounts={accounts}/>}
      </main>
    </div>
  );
}
