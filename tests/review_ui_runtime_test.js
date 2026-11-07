// Functional JavaScript/DOM harness. This is not a real browser rendering test.
const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync(process.argv[2],'utf8');
const raw=html.split('<script id="review-data" type="application/json">')[1].split('</script>')[0];
const script=html.split('<script>')[1].split('</script>')[0];
const nodes=new Map();let downloaded;
class Element{
 constructor(){this.children=[];this.value='';this.textContent='';}
 append(v){this.children.push(v);}
 replaceChildren(){this.children=[];}
 click(){if(this.onclick)this.onclick();}
}
const document={getElementById(id){if(!nodes.has(id))nodes.set(id,new Element());return nodes.get(id);},createElement(){return new Element();}};
document.getElementById('review-data').textContent=raw;
const sandbox={document,structuredClone,Blob,URL:{createObjectURL(blob){downloaded=blob;return 'blob:fixture';},revokeObjectURL(){}},setTimeout(){}};
vm.runInNewContext(script,sandbox);
const e=id=>document.getElementById(id);
assert(e('headline').textContent.includes('Microsoft'));
const selectors=e('labels').children.map(label=>label.children[0]);
for(const [i,label] of ['neutral','negative','positive'].entries()){selectors[i].value=label;selectors[i].onchange();}
e('download').onclick();assert(!downloaded);assert(e('message').textContent.includes('inceleyen'));
e('reviewer').value='simulated-client-reviewer';e('source').value='human';e('exposure').value='yes';e('download').onclick();
(async()=>{
 const saved=JSON.parse(await downloaded.text());
 assert.strictEqual(saved.articles[0].labels.prediction_seen,true);
 assert.strictEqual(saved.articles[0].labels.companies.MSFT,'positive');
 const bad=structuredClone(saved);bad.articles[0].text.headline='changed';
 await e('import').onchange({target:{files:[{size:100,text:async()=>JSON.stringify(bad)}]}});
 assert(e('message').textContent.includes('değişmiş'));
 saved.articles[0].labels.prediction_seen=false;
 await e('import').onchange({target:{files:[{size:100,text:async()=>JSON.stringify(saved)}]}});
 assert(e('message').textContent.includes('geri yüklendi'));
 e('download').onclick();const restored=JSON.parse(await downloaded.text());
 assert.strictEqual(restored.articles[0].labels.prediction_seen,false);
 fs.writeFileSync(process.argv[3],JSON.stringify(restored));
 console.log('UI DOM harness passed: render, labels, required reviewer, download, identity rejection, restore, exposure');
})().catch(e=>{console.error(e);process.exitCode=1});
