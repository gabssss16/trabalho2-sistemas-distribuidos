const test = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
function ambiente(fetch) {
 const context = {window:{location:{origin:'http://gateway',protocol:'http:'}},fetch,AbortController,AbortSignal,DOMException,URL,setTimeout,clearTimeout,console};
 vm.createContext(context);
 vm.runInContext(fs.readFileSync(__dirname+'/app.js','utf8').replace(/carregarCatalogo\(\);\s*$/,''),context);
 return context;
}
test('checkout repete 404 e usa somente Gateway',async()=>{let n=0;const c=ambiente(async url=>{assert.equal(url,'http://gateway/pedidos/42/pagamento');n++;return n<3?{ok:false,status:404}:{ok:true,json:async()=>({checkout_url:'http://mock/checkout/abc'})}});const result=await c.aguardarCheckout('/pedidos/42/pagamento',new AbortController().signal,5,0);assert.equal(result,'http://mock/checkout/abc');assert.equal(n,3)});
test('checkout encerra consultas ao trocar pedido',async()=>{let n=0;const controller=new AbortController();const c=ambiente(async()=>{n++;controller.abort();return {ok:false,status:404}});await assert.rejects(c.aguardarCheckout('/x',controller.signal,30,0));assert.equal(n,1)});
test('checkout permite nova consulta apos limite e rejeita URL insegura',async()=>{let n=0;let c=ambiente(async()=>{n++;return {ok:false,status:404}});await assert.rejects(c.aguardarCheckout('/x',new AbortController().signal,2,0),/ainda não/);assert.equal(n,2);c=ambiente(async()=>({ok:true,json:async()=>({checkout_url:'javascript:alert(1)'})}));await assert.rejects(c.aguardarCheckout('/x',new AbortController().signal,2,0),/inválido/)});
