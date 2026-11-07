"""Self-contained offline, prediction-blind human labeling page."""
import json

def review_html(review):
    payload=json.dumps(review,ensure_ascii=True).replace('<','\\u003c')
    return TEMPLATE.replace('/*REVIEW_DATA*/',payload)

TEMPLATE='''<!doctype html><html lang="tr"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>MicroBroker — İnsan incelemesi</title>
<style>body{font:16px system-ui;max-width:1000px;margin:24px auto;padding:0 20px;background:#f5f7fa;color:#172334}h1{font-size:26px}section,header{background:white;border:1px solid #dce1e8;border-radius:12px;padding:20px;margin:16px 0}label{display:block;margin:12px 0}select,input,textarea,button{font:inherit;padding:9px;border:1px solid #adb8c8;border-radius:6px}textarea{width:95%;min-height:70px}button{cursor:pointer;background:#e8eef8}pre{white-space:pre-wrap;overflow-wrap:anywhere;font:inherit;line-height:1.55}nav{display:flex;flex-wrap:wrap;gap:8px}#message{color:#9c3f0d}small{display:block;color:#526174}#headline{font-size:22px}</style>
<h1>Haber ve şirket duygusu incelemesi</h1>
<header><p>Bu sayfa model tahminlerini göstermez. Gösterilen İngilizce metni okuyup belgeyi ve her şirketi ayrı etiketle. Emin değilsen boş bırak; boş değer nötr anlamına gelmez.</p>
<small>Pozitif: olumlu gelişme/beklenti. Negatif: olumsuz gelişme/beklenti. Nötr: belirgin yön yok. Karma: hem olumlu hem olumsuz kanıt. Etiketler fiyat hareketi tahmini değildir.</small>
<nav><button id="previous">Önceki</button><button id="next">Sonraki</button><button id="unlabeled">Etiketlenmemişe git</button><button id="download">Etiketli JSON indir</button></nav>
<label>Kaydettiğin etiketleri geri yükle <input id="import" type="file" accept=".json,application/json"></label>
<p id="progress"></p><p id="message" role="status"></p></header>
<section><p id="identity"></p><h2 id="headline"></h2><h3>Özet</h3><pre id="summary"></pre><h3>İçerik</h3><pre id="content"></pre><small id="basis"></small></section>
<section><h2>Etiketler</h2><div id="labels"></div>
<label>İnceleyen <input id="reviewer" placeholder="Ad veya sabit takma ad"></label>
<label>Etiket kaynağı <select id="source"><option value="">Seç</option><option value="human">İnsan — kendim okudum</option><option value="ai_assisted">Yapay zekâ yardımıyla</option><option value="synthetic">Sentetik test</option></select></label>
<label>Bu haberin model sonucunu daha önce gördün mü? <select id="exposure"><option value="">Belirsiz</option><option value="no">Hayır</option><option value="yes">Evet</option></select></label>
<label>Notlar <textarea id="notes"></textarea></label>
<small>Etiketledikten sonra JSON indir. Çalışmayı sürdürmek için indirdiğin dosyayı yukarıdan geri yükleyebilirsin. Kaynak ve model sonucunu görme bilgisi kişisel beyanındır.</small></section>
<script id="review-data" type="application/json">/*REVIEW_DATA*/</script>
<script>
const review=JSON.parse(document.getElementById('review-data').textContent);
const baseline=structuredClone(review);let position=0,lastReviewer='',lastSource='';
const el=id=>document.getElementById(id);
const labeled=a=>a.labels.document!==null||Object.values(a.labels.companies).some(v=>v!==null);
const options=[['','Emin değilim / boş'],['positive','Pozitif'],['negative','Negatif'],['neutral','Nötr'],['mixed','Karma']];
function message(t){el('message').textContent=t;}
function selector(title,value,change){const label=document.createElement('label');label.textContent=title+' ';const s=document.createElement('select');for(const [v,t] of options){const o=document.createElement('option');o.value=v;o.textContent=t;s.append(o);}s.value=value||'';s.onchange=()=>{change(s.value||null);progress();};label.append(s);el('labels').append(label);}
function progress(){el('progress').textContent=`Haber ${position+1}/${review.articles.length} · Etiket girilen ${review.articles.filter(labeled).length}`;}
function show(){const a=review.articles[position];message('');el('identity').textContent=`Haber ID: ${a.id} · Şirketler: ${a.symbols.join(', ')}`;el('headline').textContent=a.text.headline;el('summary').textContent=a.text.summary||'Özet yok';el('content').textContent=a.text.content_text||'Tam içerik yok';el('basis').textContent=`Metin kapsamı: ${a.text_basis}; karakter sınırı uygulanan alanlar: ${a.character_truncated_fields.join(', ')||'yok'}`;el('labels').replaceChildren();selector('Belgenin tamamı',a.labels.document,v=>a.labels.document=v);for(const symbol of a.symbols)selector(symbol,a.labels.companies[symbol],v=>a.labels.companies[symbol]=v);el('reviewer').value=a.labels.reviewer||lastReviewer;el('source').value=a.labels.source||lastSource;el('exposure').value=a.labels.prediction_seen===true?'yes':a.labels.prediction_seen===false?'no':'';el('notes').value=a.labels.notes||'';progress();}
function save(){const a=review.articles[position];lastReviewer=el('reviewer').value.trim();lastSource=el('source').value;a.labels.reviewer=lastReviewer||null;a.labels.source=lastSource||null;a.labels.prediction_seen=el('exposure').value==='yes'?true:el('exposure').value==='no'?false:null;a.labels.notes=el('notes').value;}
for(const id of ['reviewer','source','exposure','notes'])el(id).oninput=save;
el('previous').onclick=()=>{save();position=Math.max(0,position-1);show();};el('next').onclick=()=>{save();position=Math.min(review.articles.length-1,position+1);show();};
el('unlabeled').onclick=()=>{save();const start=position;for(let n=1;n<=review.articles.length;n++){const i=(start+n)%review.articles.length;if(!labeled(review.articles[i])){position=i;show();return;}}message('Tüm haberlere en az bir etiket girilmiş.');};
el('download').onclick=()=>{save();const missing=review.articles.find(a=>labeled(a)&&(!a.labels.reviewer||!a.labels.source));if(missing){position=review.articles.indexOf(missing);show();message('Etiketli haber için inceleyen ve kaynak seçilmeli.');return;}const blob=new Blob([JSON.stringify(review,null,2)+'\\n'],{type:'application/json'});const url=URL.createObjectURL(blob);const link=document.createElement('a');link.href=url;link.download='review-labeled.json';link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);message('Etiketler indirildi. Dosyayı evaluate-analysis-review --labels-file ile değerlendirebilirsin.');};
function stable(v){if(Array.isArray(v))return v.map(stable);if(v&&typeof v==='object')return Object.fromEntries(Object.keys(v).sort().map(k=>[k,stable(v[k])]));return v;}
function identity(a){return JSON.stringify(stable(Object.fromEntries(Object.entries(a).filter(([k])=>k!=='labels'))));}
el('import').onchange=async e=>{try{const f=e.target.files[0];if(!f)return;if(f.size>32*1024*1024)throw Error('Dosya sınırı 32 MiB.');const loaded=JSON.parse(await f.text());if(loaded.format!==baseline.format||loaded.reference_sha256!==baseline.reference_sha256||!Array.isArray(loaded.articles)||loaded.articles.length!==baseline.articles.length)throw Error('İnceleme kimliği eşleşmiyor.');for(let i=0;i<loaded.articles.length;i++){const a=loaded.articles[i],b=baseline.articles[i];if(identity(a)!==identity(b)||!a.labels||!a.labels.companies||Object.keys(a.labels.companies).sort().join()!==b.symbols.slice().sort().join())throw Error('Haber metni veya şirketler değişmiş.');for(const v of [a.labels.document,...Object.values(a.labels.companies)])if(v!==null&&!options.some(([key])=>key&&key===v))throw Error('Etiket geçersiz.');if(a.labels.prediction_seen!==undefined&&a.labels.prediction_seen!==null&&typeof a.labels.prediction_seen!=='boolean')throw Error('Model sonucunu görme bilgisi geçersiz.');if(a.labels.reviewer!==null&&typeof a.labels.reviewer!=='string')throw Error('İnceleyen geçersiz.');if(a.labels.source!==null&&!['human','ai_assisted','synthetic'].includes(a.labels.source))throw Error('Kaynak geçersiz.');if(typeof a.labels.notes!=='string')throw Error('Notlar geçersiz.');}for(let i=0;i<review.articles.length;i++)review.articles[i].labels=loaded.articles[i].labels;show();message('Etiketler geri yüklendi.');}catch(error){message(error.message);}};
show();
</script></html>'''
