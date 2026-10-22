const API_URL = window.location.protocol === 'file:' ? 'http://localhost:8080' : window.location.origin;
let eventoSSE = null;
let consultaPagamento = null;
let pedidoAtual = null;

async function requisitar(url, options = {}) {
    const resposta = await fetch(`${API_URL}${url}`, options);
    const corpo = await resposta.text();
    if (!resposta.ok) throw new Error(`API respondeu ${resposta.status}: ${corpo.trim() || resposta.statusText}`);
    try { return JSON.parse(corpo); } catch { throw new Error('A API retornou uma resposta inválida.'); }
}
async function gerenciarInteresse(metodo) {
    const email = document.getElementById('email-promo').value.trim();
    const categorias = Array.from(document.querySelectorAll('.cat-checkbox:checked')).map(c => c.value);
    if (!email || (metodo === 'POST' && categorias.length === 0)) {
        alert('Informe seu e-mail e selecione as categorias para se cadastrar.'); return;
    }
    try {
        await requisitar('/interesse', {method: metodo, headers: {'Content-Type': 'application/json'}, body: JSON.stringify({email, categorias})});
        alert(metodo === 'DELETE' ? 'Cancelamento enviado para este e-mail.' : 'Cadastro enviado para este e-mail.');
    } catch (erro) { alert(erro.message); }
}
async function carregarCatalogo() {
    const lista = document.getElementById('lista-produtos');
    try {
        const produtos = await requisitar('/produtos');
        lista.replaceChildren();
        if (!produtos.length) { lista.textContent = 'Nenhum produto em estoque.'; return; }
        for (const p of produtos) {
            const linha = document.createElement('div'); linha.className = 'produto';
            const nome = document.createElement('span'); nome.textContent = `${p.name} (Estoque: ${p.quantity})`;
            const formulario = document.createElement('form'); formulario.className = 'compra';
            const rotulo = document.createElement('label'); rotulo.htmlFor = `quantidade-${p.product_id}`; rotulo.textContent = 'Quantidade';
            const quantidade = document.createElement('input');
            quantidade.id = rotulo.htmlFor; quantidade.type = 'number'; quantidade.min = '1';
            quantidade.max = String(p.quantity); quantidade.step = '1'; quantidade.value = '1'; quantidade.required = true;
            const botao = document.createElement('button'); botao.type = 'submit'; botao.textContent = 'Comprar';
            formulario.onsubmit = async event => {
                event.preventDefault();
                if (!formulario.reportValidity()) return;
                botao.disabled = true;
                try { await comprar(p.product_id, Number(quantidade.value)); }
                finally { botao.disabled = false; }
            };
            formulario.append(rotulo, quantidade, botao);
            linha.append(nome, formulario); lista.append(linha);
        }
    } catch (erro) { lista.textContent = `Não foi possível carregar o catálogo: ${erro.message}`; }
}
async function comprar(idProduto, quantidade) {
    try {
        if (!Number.isSafeInteger(quantidade) || quantidade < 1) throw new Error('Informe uma quantidade inteira maior que zero.');
        const pedido = await requisitar('/pedidos', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({itens: [{product_id: idProduto, quantity: quantidade}]})});
        if (consultaPagamento) consultaPagamento.abort();
        consultaPagamento = null;
        pedidoAtual = pedido.order_id;
        document.getElementById('status-container').style.display = 'block';
        document.getElementById('area-pagamento').replaceChildren();
        document.getElementById('numero-pedido').textContent = `#${pedido.order_id}`;
        document.getElementById('status-texto').textContent = pedido.status;
        const cancelar = document.getElementById('cancelar-pedido');
        cancelar.hidden = !pedido._links.cancelar;
        cancelar.disabled = false;
        cancelar.onclick = async () => {
            cancelar.disabled = true;
            try {
                await requisitar(pedido._links.cancelar, {method: 'POST'});
                if (pedidoAtual === pedido.order_id) cancelar.hidden = true;
            } catch (erro) { alert(erro.message); cancelar.disabled = false; }
        };
        conectarSSE(
            pedido._links.acompanhar_sse, 
            pedido.order_id, 
            pedido._links.pagamento
        );
    } catch (erro) { console.error(erro); alert(`Falha ao criar o pedido: ${erro.message}`); }
}
function conectarSSE(url, orderId, pagamentoURL) {
    if (eventoSSE) eventoSSE.close();
    const conexao = new EventSource(`${API_URL}${url}`); eventoSSE = conexao;
    conexao.onmessage = event => {
        if (pedidoAtual !== orderId) return;
        const status = event.data;
        document.getElementById('status-texto').textContent = status;
        if (['pedido.cancelamento_solicitado', 'pagamento.aprovado', 'pedido.enviado', 'pedido.excluido', 'estoque.indisponivel', 'pagamento.recusado'].includes(status)) {
            document.getElementById('cancelar-pedido').hidden = true;
        }
        if (status === 'pedido.estoque_ok' && !consultaPagamento) buscarLinkPagamento(orderId, pagamentoURL);
        if (['pedido.cancelamento_solicitado', 'pagamento.aprovado', 'pedido.enviado', 'pedido.excluido', 'estoque.indisponivel', 'pagamento.recusado'].includes(status)) {
            if (consultaPagamento) consultaPagamento.abort();
            document.getElementById('area-pagamento').replaceChildren();
        }
        if (['pedido.enviado', 'pedido.excluido', 'estoque.indisponivel', 'pagamento.recusado'].includes(status)) {
            conexao.close(); carregarCatalogo();
        }
    };
    // EventSource reconecta e envia Last-Event-ID; o servidor recupera o historico.
    conexao.onerror = () => { if (pedidoAtual === orderId) document.getElementById('status-texto').textContent = 'Reconectando ao acompanhamento…'; };
}
function esperar(ms, signal) {
    return new Promise((resolve, reject) => {
        if (signal.aborted) { reject(new DOMException('Cancelado', 'AbortError')); return; }
        const abortar = () => { clearTimeout(timer); reject(new DOMException('Cancelado', 'AbortError')); };
        const timer = setTimeout(() => { signal.removeEventListener('abort', abortar); resolve(); }, ms);
        signal.addEventListener('abort', abortar, {once: true});
    });
}
// 404 indica que o consumidor ainda nao criou a cobranca. Falhas temporarias
// tambem sao repetidas; cada consulta tem timeout, alem do limite de tentativas.
async function aguardarCheckout(url, signal, tentativas = 30, pausa = 1000) {
    for (let i = 0; i < tentativas; i++) {
        if (signal.aborted) throw new DOMException('Cancelado', 'AbortError');
        try {
            const resposta = await fetch(`${API_URL}${url}`, {signal: AbortSignal.any([signal, AbortSignal.timeout(5000)])});
            if (resposta.ok) {
                const cobranca = await resposta.json();
                if (!cobranca.checkout_url) throw new Error('A cobrança não possui link de pagamento.');
                const checkout = new URL(cobranca.checkout_url);
                if (!['http:', 'https:'].includes(checkout.protocol)) throw new Error('Link de pagamento inválido.');
                return checkout.href;
            }
            if (resposta.status !== 404 && resposta.status < 500) throw new Error(`Pedido indisponível para pagamento (${resposta.status}).`);
        } catch (erro) {
            if (signal.aborted || (erro.name !== 'TypeError' && erro.name !== 'TimeoutError')) throw erro;
        }
        if (i + 1 < tentativas) await esperar(pausa, signal);
    }
    throw new Error('O pagamento ainda não ficou disponível. Tente consultar novamente.');
}
async function buscarLinkPagamento(orderId, url) {
    if (consultaPagamento) consultaPagamento.abort();
    const controle = new AbortController(); consultaPagamento = controle;
    const area = document.getElementById('area-pagamento'); area.textContent = 'Preparando o pagamento…';
    try {
        const checkout = await aguardarCheckout(url, controle.signal);
        if (controle.signal.aborted || pedidoAtual !== orderId) return;
        const link = document.createElement('a'); link.href = checkout; link.target = '_blank'; link.rel = 'noopener'; link.textContent = 'Pagar no Mock Externo ↗';
        area.replaceChildren(link);
    } catch (erro) {
        if (controle.signal.aborted || pedidoAtual !== orderId) return;
        area.textContent = erro.message + ' ';
        const botao = document.createElement('button'); botao.textContent = 'Consultar pagamento novamente'; botao.onclick = () => buscarLinkPagamento(orderId, url); area.append(botao);
    } finally { if (consultaPagamento === controle) consultaPagamento = null; }
}
carregarCatalogo();
