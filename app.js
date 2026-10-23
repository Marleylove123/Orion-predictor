(() => {
"use strict";
const $ = id => document.getElementById(id);
const state = {
  sockets: new Map(), ticks: new Map(), lastPrediction: new Map(),
  rows: [], pendingHistory: new Set(), connected: false, generation: 0,
  stats: {predictions:0,hits:0,signals:0,signalHits:0}
};
const WINDOW = () => Math.max(20, Math.min(1000, Number($("windowSize").value)||1000));
const THRESHOLD = () => Math.max(10, Math.min(100, Number($("threshold").value)||90))/100;
function setStatus(s, bad=false){$("status").textContent=s; $("status").className="status"+(bad?" warn":"");}
function setConn(s){$("conn").textContent=s;}
function selectedMarkets(){return [...$("markets").selectedOptions].map(o=>o.value);}
function digitFromQuote(quote, pipSize){
  // Use Deriv's pip_size when available to preserve displayed final decimal digit.
  const decimals = Number.isInteger(pipSize) ? Math.max(0, Math.min(10,pipSize)) : null;
  let s;
  if(decimals !== null) s = Number(quote).toFixed(decimals);
  else s = String(quote);
  const clean = s.replace(/\D/g,"");
  return clean.length ? Number(clean.at(-1)) : null;
}
function addTick(symbol, quote, epoch, pipSize){
  const digit=digitFromQuote(quote,pipSize);
  if(digit===null) return;
  const arr=state.ticks.get(symbol)||[];
  const last=arr.at(-1);
  if(last && Number(epoch)===last.epoch && Number(quote)===last.quote) return;
  // Score the prior tick's prediction against this newly arrived tick.
  const prior=state.lastPrediction.get(symbol);
  if(prior && prior.epoch !== Number(epoch)){
    const hit=prior.digit===digit;
    state.stats.predictions++; if(hit)state.stats.hits++;
    if(prior.signal){state.stats.signals++;if(hit)state.stats.signalHits++;}
    pushRow({time:new Date(Number(epoch)*1000).toISOString(),market:symbol,pred:prior.digit,prob:prior.prob,actual:digit,result:hit?"HIT":"MISS",type:"LIVE"});
    state.lastPrediction.delete(symbol);
  }
  arr.push({quote:Number(quote),epoch:Number(epoch),digit});
  if(arr.length>1500)arr.splice(0,arr.length-1500);
  state.ticks.set(symbol,arr);
  makePrediction(symbol, "LIVE");
  render(symbol);
}
function frequency(arr){
  const c=Array(10).fill(0);
  for(const x of arr)c[x.digit]++;
  return c.map(n=>n/(arr.length||1));
}
function makePrediction(symbol, type){
  const arr=state.ticks.get(symbol)||[];
  const w=arr.slice(-WINDOW());
  if(w.length<100)return;
  const f=frequency(w);
  // Baseline chooses the historically most frequent digit; ties break by lower digit.
  let best=0; for(let d=1;d<10;d++)if(f[d]>f[best])best=d;
  const prob=f[best], signal=prob>=THRESHOLD();
  state.lastPrediction.set(symbol,{digit:best,prob,signal,epoch:arr.at(-1).epoch});
  if(type==="BACKTEST"){
    pushRow({time:new Date(arr.at(-1).epoch*1000).toISOString(),market:symbol,pred:best,prob,actual:"—",result:signal?"QUALIFIES":"NO SIGNAL",type:"BACKTEST"});
  }
  if(symbol===selectedMarkets()[0]){
    $("sigMarket").textContent=symbol;$("sigDigit").textContent=best;
    $("sigProb").textContent=(prob*100).toFixed(1)+"%";
    $("sigState").textContent=signal?"Threshold met; wait for the next tick to score it.":"No qualifying signal: estimate is below "+(THRESHOLD()*100).toFixed(0)+"%.";
    $("sigState").className="status "+(signal?"good":"warn");
  }
}
function pushRow(r){
  state.rows.unshift(r);if(state.rows.length>500)state.rows.length=500;
  const tr=document.createElement("tr");
  [r.time,r.market,r.pred,typeof r.prob==="number"?(r.prob*100).toFixed(1)+"%":r.prob,r.actual,r.result,r.type].forEach(v=>{
    const td=document.createElement("td");td.textContent=String(v);tr.appendChild(td);
  });
  $("log").prepend(tr);
  while($("log").children.length>100)$("log").lastElementChild.remove();
  updateStats();
}
function updateStats(){
  $("nPred").textContent=state.stats.predictions;
  $("hitRate").textContent=state.stats.predictions?(100*state.stats.hits/state.stats.predictions).toFixed(2)+"%":"—";
  $("sigStats").textContent=state.stats.signals+" / "+(state.stats.signals?(100*state.stats.signalHits/state.stats.signals).toFixed(2)+"%":"—");
  $("nTicks").textContent=[...state.ticks.values()].reduce((n,a)=>n+a.length,0);
}
function render(symbol){
  const arr=state.ticks.get(symbol)||[], f=frequency(arr.slice(-WINDOW()));
  const wrap=$("digits");wrap.replaceChildren();
  f.forEach((p,d)=>{const el=document.createElement("div");el.className="digit";const b=document.createElement("b");b.textContent=d;const sm=document.createElement("small");sm.textContent=(p*100).toFixed(1)+"%";el.append(b,sm);wrap.append(el);});
  updateStats();
}
function wsUrl(){
  const id=$("appId").value.trim();
  return "wss://ws.derivws.com/websockets/v3"+(id?"?app_id="+encodeURIComponent(id):"");
}
function send(ws,obj){if(ws.readyState===WebSocket.OPEN)ws.send(JSON.stringify(obj));}
function closeAll(){
  state.generation++;
  for(const ws of state.sockets.values()){try{ws.close();}catch{}}
  state.sockets.clear();state.connected=false;setConn("Disconnected");
}
function connectOne(symbol,generation){
  return new Promise((resolve)=>{
    const ws=new WebSocket(wsUrl());state.sockets.set(symbol,ws);
    let gotHistory=false;
    ws.onopen=()=>{
      send(ws,{ticks_history:symbol,count:1000,end:"latest",style:"ticks",req_id:1});
    };
    ws.onmessage=ev=>{
      let d;try{d=JSON.parse(ev.data)}catch{return}
      if(d.error){setStatus(symbol+": "+d.error.message,true);return;}
      if(generation!==state.generation)return;
      if(d.msg_type==="history" && !gotHistory){
        gotHistory=true;
        const prices=d.history?.prices||[], times=d.history?.times||[];
        const pipSize=d.pip_size;
        const arr=[];
        for(let i=0;i<prices.length;i++){const digit=digitFromQuote(prices[i],pipSize);if(digit!==null)arr.push({quote:Number(prices[i]),epoch:Number(times[i]),digit});}
        state.ticks.set(symbol,arr.slice(-1500));
        runBacktest(symbol,arr);
        render(symbol);
        send(ws,{ticks:symbol,subscribe:1,req_id:2});
        setStatus("Loaded "+arr.length+" historical ticks for "+symbol+"; live stream requested.");
        resolve(true);
      } else if(d.msg_type==="tick" && d.tick){
        addTick(symbol,d.tick.quote,d.tick.epoch,d.tick.pip_size);
      }
    };
    ws.onerror=()=>{setStatus("Connection error for "+symbol+". Check network, App ID, and whether this symbol is available.",true);resolve(false);};
    ws.onclose=()=>{if(state.sockets.get(symbol)===ws)state.sockets.delete(symbol);};
    setTimeout(()=>{if(!gotHistory)resolve(false)},12000);
  });
}
function runBacktest(symbol,all){
  // Walk-forward: for each test point, predict from the preceding WINDOW ticks only.
  const warmup=Math.min(100,Math.floor(all.length/3));
  if(all.length<=warmup+1){setStatus("Not enough history for backtest on "+symbol,true);return;}
  let n=0,hits=0,sigN=0,sigHits=0;
  const step=Math.max(1,Math.floor((all.length-warmup)/100));
  for(let i=warmup;i<all.length;i+=step){
    const prior=all.slice(Math.max(0,i-WINDOW()),i);
    const f=frequency(prior);let best=0;
    for(let d=1;d<10;d++)if(f[d]>f[best])best=d;
    const hit=best===all[i].digit, signal=f[best]>=THRESHOLD();
    n++;if(hit)hits++;if(signal){sigN++;if(hit)sigHits++;}
    if(i===warmup || i+step>=all.length)pushRow({time:new Date(all[i].epoch*1000).toISOString(),market:symbol,pred:best,prob:f[best],actual:all[i].digit,result:hit?"HIT":"MISS",type:"BACKTEST"});
  }
  state.stats.predictions+=n;state.stats.hits+=hits;state.stats.signals+=sigN;state.stats.signalHits+=sigHits;
  updateStats();
  setStatus("Backtest "+symbol+": "+n+" walk-forward predictions, "+(100*hits/n).toFixed(1)+"% exact-digit hit rate; "+sigN+" met the configured threshold. Results are historical and not a guarantee.");
}
$("connect").addEventListener("click",async()=>{
  closeAll();state.ticks.clear();state.lastPrediction.clear();state.rows=[];state.stats={predictions:0,hits:0,signals:0,signalHits:0};$("log").replaceChildren();
  const markets=selectedMarkets();if(!markets.length){setStatus("Select at least one market.",true);return;}
  const gen=state.generation;setConn("Connecting…");setStatus("Connecting to Deriv public market data…");
  let success=0;
  for(const s of markets){if(gen!==state.generation)break; if(await connectOne(s,gen))success++;}
  if(gen===state.generation){state.connected=success>0;setConn(state.connected?"Connected":"Connection failed");if(!success)setStatus("No selected market connected. Try one market at a time or check your App ID.",true);}
});
$("stop").addEventListener("click",()=>{closeAll();setStatus("Disconnected. Existing session statistics remain until reset.");});
$("reset").addEventListener("click",()=>{state.stats={predictions:0,hits:0,signals:0,signalHits:0};state.rows=[];$("log").replaceChildren();updateStats();setStatus("Session statistics reset.");});
$("export").addEventListener("click",()=>{
  const rows=[["time","market","predicted_digit","estimated_frequency","actual_digit","result","type"],...state.rows.map(r=>[r.time,r.market,r.pred,typeof r.prob==="number"?(r.prob*100).toFixed(3)+"%":r.prob,r.actual,r.result,r.type])];
  const csv=rows.map(row=>row.map(v=>'"'+String(v??"").replace(/"/g,'""')+'"').join(",")).join("\r\n");
  const blob=new Blob([csv],{type:"text/csv;charset=utf-8"}),url=URL.createObjectURL(blob),a=document.createElement("a");
  a.href=url;a.download="orion-predictor-results.csv";a.click();URL.revokeObjectURL(url);
});
$("threshold").addEventListener("change",()=>{for(const s of state.ticks.keys())makePrediction(s,"LIVE");});
$("windowSize").addEventListener("change",()=>{for(const s of state.ticks.keys()){makePrediction(s,"LIVE");render(s);}});
$("markets").addEventListener("change",()=>{const s=selectedMarkets()[0];if(s&&state.ticks.has(s)){makePrediction(s,"LIVE");render(s);}});
render("");
})();