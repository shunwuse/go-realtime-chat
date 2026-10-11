package main

import (
	"net/http"
)

// page is a minimal manual-test client: pick room and name, chat, watch event ids.
// Dual-window test: open it twice (normal + incognito) with different names.
const page = `<!doctype html>
<html><body>
room <input id="room" value="general">
name <input id="from" value="alice">
last <input id="since" value="" placeholder="last_event_id">
<button onclick="join()">join</button>
<div id="log"></div>
<input id="body" placeholder="message"><button onclick="send()">send</button>
<script>
let ws, last = 0;
function join() {
    const room = document.getElementById('room').value;
    const from = document.getElementById('from').value;
    const since = document.getElementById('since').value;
    let url = 'ws://' + location.host + '/ws?room=' + room + '&from=' + from;
    if (since) url += '&last_event_id=' + since;
    ws = new WebSocket(url);
    ws.onmessage = (e) => {
        const m = JSON.parse(e.data);
        if (m.event_id) last = m.event_id;
        document.getElementById('log').innerHTML += '<div>' + (m.from || '?') + ': ' + (m.body || m.error) + ' [' + (m.event_id || '-') + ']</div>';
        document.getElementById('since').value = last;
    };
}
function send() {
    ws.send(JSON.stringify({body: document.getElementById('body').value}));
}
</script>
</body></html>`

func servePage(w http.ResponseWriter, request *http.Request) {
	if request.URL.Path != "/" {
		http.NotFound(w, request)

		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(page))
}
