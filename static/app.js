import { createApp, ref, computed, onMounted } from 'https://unpkg.com/vue@3/dist/vue.esm-browser.prod.js'

const app = createApp({
  setup(){
    const list = ref('')
    const concurrency = ref(200)
    const timeout = ref(7)
    const status = ref('loading...')
    const results = ref([])
    const total = ref(0)
    const checked = ref(0)
    const found = ref(0)
    const prog = ref(0)
    const controller = ref(null)
    const anonymity = ref('all')
    const ptype = ref('all')
    const sortKey = ref('latency_ms')
    const sortDir = ref('asc')
    const currentPage = ref(1)
    const pageSize = ref(25)

    function pushResult(r){
      results.value.unshift(r)
      checked.value++
      if (r.ok) found.value++
      prog.value = total.value ? Math.min(100, Math.round((checked.value/total.value)*100)) : 0
    }

    async function fetchStatus(){
      try{
        const r = await fetch('/status')
        if (!r.ok) return
        const j = await r.json()
        status.value = `good:${j.good_count} workers:${j.workers} timeout:${j.timeout} ip:${j.own_ip||'unknown'}`
      }catch(e){ status.value = 'offline' }
    }

    async function downloadGood(){
      try{
        const r = await fetch('/good')
        if (!r.ok) return alert('no good list')
        const txt = await r.text()
        const blob = new Blob([txt], {type:'text/plain'})
        const a = document.createElement('a')
        a.href = URL.createObjectURL(blob)
        a.download = 'good.txt'
        document.body.appendChild(a); a.click(); a.remove()
      }catch(e){ alert('download failed') }
    }

    function resetStats(){ checked.value = 0; found.value = 0; prog.value = 0; results.value = []; currentPage.value = 1 }

    const filtered = computed(()=> results.value.filter(i=>{
      if (anonymity.value !== 'all' && (i.anonymity||'') !== anonymity.value) return false
      if (ptype.value !== 'all' && (i.type||'') !== ptype.value) return false
      return true
    }))

    const sorted = computed(()=>{
      const arr = [...filtered.value]
      arr.sort((a,b)=>{
        const A = a[sortKey.value] ?? ''
        const B = b[sortKey.value] ?? ''
        let cmp = 0
        if (typeof A === 'number' && typeof B === 'number') cmp = A - B
        else cmp = String(A).localeCompare(String(B))
        return sortDir.value === 'asc' ? cmp : -cmp
      })
      return arr
    })

    const totalPages = computed(()=> Math.max(1, Math.ceil(sorted.value.length / pageSize.value)))
    const pageItems = computed(()=>{
      const s = (currentPage.value-1)*pageSize.value
      return sorted.value.slice(s, s+pageSize.value)
    })

    async function start(){
      if (controller.value) return
      if (!list.value.trim()) return alert('paste proxies')
      resetStats()
      controller.value = new AbortController()
      total.value = list.value.split('\n').filter(l=>l.trim()).length

      try{
        const resp = await fetch(`/check?c=${concurrency.value}&t=${timeout.value}`, {method:'POST', body:list.value, signal: controller.value.signal})
        if (!resp.ok){ const txt = await resp.text(); alert(txt); controller.value = null; return }
        const reader = resp.body.getReader(); const dec = new TextDecoder(); let buf = ''
        while(true){
          const {done, value} = await reader.read()
          if (done) break
          buf += dec.decode(value, {stream:true})
          let parts = buf.split('\n\n')
          buf = parts.pop()
          for(const part of parts){
            const line = part.trim().split('\n').find(l=>l.startsWith('data:'))
            if (!line) continue
            try{ const obj = JSON.parse(line.replace(/^data:\s*/,'')); pushResult(obj) }catch(e){console.error('parse',e)}
          }
        }
      }catch(e){ if (e.name !== 'AbortError') console.error(e) }
      finally{ controller.value = null }
    }

    function stop(){ if (controller.value) controller.value.abort(); controller.value = null }

    function toggleSort(key){
      if (sortKey.value === key) sortDir.value = sortDir.value === 'asc' ? 'desc' : 'asc'
      else { sortKey.value = key; sortDir.value = 'asc' }
      currentPage.value = 1
    }

    function prevPage(){ if (currentPage.value > 1) currentPage.value-- }
    function nextPage(){ if (currentPage.value < totalPages.value) currentPage.value++ }

    onMounted(()=>{ fetchStatus(); setInterval(fetchStatus, 15000) })

    return { list, concurrency, timeout, status, results, total, checked, found, prog, anonymity, ptype, sortKey, sortDir, currentPage, pageSize, pageItems, totalPages, start, stop, downloadGood, toggleSort, prevPage, nextPage }
  }
})

app.mount('#app')
