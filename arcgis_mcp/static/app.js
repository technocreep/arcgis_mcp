const { createApp, ref, computed, onMounted, nextTick, watch } = Vue

// ═══════════════════════════════════════════════
// JSON Tree renderer (pure DOM, no framework)
// ═══════════════════════════════════════════════

function buildJsonNode(key, value, depth, defaultExpanded) {
    const isRoot = key === null
    const isObj = value !== null && typeof value === 'object' && !Array.isArray(value)
    const isArr = Array.isArray(value)
    const isExpandable = isObj || isArr

    const wrapper = document.createElement('div')
    wrapper.className = 'json-node'

    if (isExpandable) {
        const childKeys = isObj ? Object.keys(value) : value.map((_, i) => i)
        const count = childKeys.length
        const bracket = isArr ? ['[', ']'] : ['{', '}']
        const expanded = { value: defaultExpanded }

        // Toggle row
        const row = document.createElement('div')
        row.className = 'json-toggle py-0.5'

        const arrow = document.createElement('span')
        arrow.className = 'json-arrow'
        arrow.textContent = expanded.value ? '▼' : '▶'
        row.appendChild(arrow)

        if (!isRoot) {
            const keySpan = document.createElement('span')
            keySpan.className = 'json-key'
            keySpan.textContent = JSON.stringify(key)
            row.appendChild(keySpan)

            const colon = document.createElement('span')
            colon.className = 'text-slate-400'
            colon.textContent = ': '
            row.appendChild(colon)
        }

        const openBracket = document.createElement('span')
        openBracket.className = 'text-slate-500 font-mono'
        openBracket.textContent = bracket[0]
        row.appendChild(openBracket)

        const preview = document.createElement('span')
        preview.className = 'json-meta ml-1'
        preview.textContent = expanded.value
            ? ''
            : (isArr ? `${count} items` : `${count} keys`)
        row.appendChild(preview)

        wrapper.appendChild(row)

        // Children container
        const children = document.createElement('div')
        children.className = 'json-children'
        children.style.display = expanded.value ? 'block' : 'none'

        // Rebuild to avoid duplication
        children.innerHTML = ''
        childKeys.forEach((k) => {
            const v = isArr ? value[k] : value[k]
            const childNode = buildJsonNode(isArr ? k : k, v, depth + 1, depth < 1)
            children.appendChild(childNode)
        })

        const closeLine = document.createElement('div')
        closeLine.className = 'text-slate-500 font-mono'
        closeLine.textContent = bracket[1]

        wrapper.appendChild(children)
        wrapper.appendChild(closeLine)

        // Toggle handler
        row.addEventListener('click', () => {
            expanded.value = !expanded.value
            arrow.textContent = expanded.value ? '▼' : '▶'
            children.style.display = expanded.value ? 'block' : 'none'
            preview.textContent = expanded.value
                ? ''
                : (isArr ? `${count} items` : `${count} keys`)
        })

        // Store references for expand/collapse all
        wrapper.dataset.expandable = 'true'
        wrapper._toggleExpand = (forceExpand) => {
            expanded.value = forceExpand
            arrow.textContent = expanded.value ? '▼' : '▶'
            children.style.display = expanded.value ? 'block' : 'none'
            preview.textContent = expanded.value
                ? ''
                : (isArr ? `${count} items` : `${count} keys`)
        }

    } else {
        // Leaf node
        const row = document.createElement('div')
        row.className = 'py-0.5 flex flex-wrap gap-1'

        if (!isRoot && key !== null) {
            const keySpan = document.createElement('span')
            keySpan.className = 'json-key'
            keySpan.textContent = JSON.stringify(key)
            row.appendChild(keySpan)

            const colon = document.createElement('span')
            colon.className = 'text-slate-400'
            colon.textContent = ': '
            row.appendChild(colon)
        }

        const valSpan = document.createElement('span')
        if (value === null) {
            valSpan.className = 'json-null'
            valSpan.textContent = 'null'
        } else if (typeof value === 'boolean') {
            valSpan.className = 'json-bool'
            valSpan.textContent = String(value)
        } else if (typeof value === 'number') {
            valSpan.className = 'json-num'
            valSpan.textContent = String(value)
        } else {
            valSpan.className = 'json-str'
            const str = JSON.stringify(value)
            valSpan.textContent = str.length > 200 ? str.slice(0, 200) + '…"' : str
            valSpan.title = str.length > 200 ? value : ''
        }
        row.appendChild(valSpan)
        wrapper.appendChild(row)
    }

    return wrapper
}

function renderJsonTree(container, data) {
    container.innerHTML = ''
    const root = buildJsonNode(null, data, 0, true)
    container.appendChild(root)
}

function toggleAllNodes(container, expand) {
    container.querySelectorAll('[data-expandable]').forEach(el => {
        if (el._toggleExpand) el._toggleExpand(expand)
    })
}

// ═══════════════════════════════════════════════
// Vue App
// ═══════════════════════════════════════════════

createApp({
    setup() {
        // ── Auth state ──
        const isAuthenticated = ref(false)
        const authUser = ref(localStorage.getItem('gis_auth_user') || '')
        const authPass = ref(localStorage.getItem('gis_auth_pass') || '')

        const authHeader = computed(() => {
            if (!authUser.value || !authPass.value) return null
            return 'Basic ' + btoa(authUser.value + ':' + authPass.value)
        })

        // ── Login form ──
        const loginForm = ref({ user: '', pass: '' })
        const loginLoading = ref(false)
        const loginError = ref(null)

        const signIn = async () => {
            loginLoading.value = true
            loginError.value = null
            const header = 'Basic ' + btoa(loginForm.value.user + ':' + loginForm.value.pass)
            try {
                const res = await fetch('/api/auth', { headers: { Authorization: header } })
                if (res.ok) {
                    authUser.value = loginForm.value.user
                    authPass.value = loginForm.value.pass
                    localStorage.setItem('gis_auth_user', authUser.value)
                    localStorage.setItem('gis_auth_pass', authPass.value)
                    isAuthenticated.value = true
                    fetchProjects()
                } else {
                    loginError.value = 'Invalid username or password.'
                }
            } catch {
                loginError.value = 'Server unreachable.'
            } finally {
                loginLoading.value = false
            }
        }

        // ── Change credentials modal ──
        const showAuthModal = ref(false)
        const authDraft = ref({ user: '', pass: '' })
        const authModalError = ref(null)
        const authModalLoading = ref(false)

        const openAuthModal = () => {
            authDraft.value = { user: authUser.value, pass: authPass.value }
            authModalError.value = null
            showAuthModal.value = true
        }

        const saveCredentials = async () => {
            authModalLoading.value = true
            authModalError.value = null
            const header = 'Basic ' + btoa(authDraft.value.user + ':' + authDraft.value.pass)
            try {
                const res = await fetch('/api/auth', { headers: { Authorization: header } })
                if (res.ok) {
                    authUser.value = authDraft.value.user
                    authPass.value = authDraft.value.pass
                    localStorage.setItem('gis_auth_user', authUser.value)
                    localStorage.setItem('gis_auth_pass', authPass.value)
                    showAuthModal.value = false
                } else {
                    authModalError.value = 'Invalid credentials.'
                }
            } catch {
                authModalError.value = 'Server unreachable.'
            } finally {
                authModalLoading.value = false
            }
        }

        // ── Projects ──
        const projects = ref([])
        const loading = ref(true)
        const showUploadModal = ref(false)
        const uploading = ref(false)
        const error = ref(null)

        const form = ref({ id: '' })
        const gdbInput = ref(null)
        const aprxInput = ref(null)
        const atbxInput = ref(null)

        const fetchProjects = async () => {
            loading.value = true
            try {
                const res = await fetch('/api/projects')
                const data = await res.json()
                projects.value = data.projects
            } catch (e) {
                console.error(e)
            } finally {
                loading.value = false
            }
        }

        const uploadProject = async () => {
            uploading.value = true
            error.value = null

            const formData = new FormData()
            formData.append('project_id', form.value.id)
            if (gdbInput.value?.files[0]) formData.append('gdb_zip', gdbInput.value.files[0])
            if (aprxInput.value?.files[0]) formData.append('aprx', aprxInput.value.files[0])
            if (atbxInput.value?.files[0]) formData.append('atbx', atbxInput.value.files[0])

            try {
                const res = await fetch('/api/upload', {
                    method: 'POST',
                    headers: { Authorization: authHeader.value },
                    body: formData
                })
                if (res.status === 401) throw new Error('Invalid credentials. Please re-authenticate (lock icon).')
                if (!res.ok) {
                    const err = await res.json().catch(() => ({}))
                    throw new Error(err.detail || 'Upload failed')
                }
                showUploadModal.value = false
                form.value.id = ''
                if (gdbInput.value) gdbInput.value.value = ''
                if (aprxInput.value) aprxInput.value.value = ''
                if (atbxInput.value) atbxInput.value.value = ''
                await fetchProjects()
            } catch (e) {
                error.value = e.message
            } finally {
                uploading.value = false
            }
        }

        const deleteProject = async (id) => {
            if (!confirm(`Delete project '${id}'?`)) return
            try {
                const res = await fetch(`/api/projects/${id}`, {
                    method: 'DELETE',
                    headers: { Authorization: authHeader.value }
                })
                if (res.status === 401) { alert('Invalid credentials.'); return }
                if (res.ok) await fetchProjects()
            } catch { alert('Failed to delete') }
        }

        const formatDate = (isoString) => {
            if (!isoString) return 'Unknown date'
            return new Date(isoString).toLocaleDateString(undefined, {
                year: 'numeric', month: 'short', day: 'numeric',
                hour: '2-digit', minute: '2-digit'
            })
        }

        // ── Observe ──
        const showObserveModal = ref(false)
        const observeData = ref(null)
        const observeLoading = ref(false)
        const observeProject = ref(null)
        const treeContainer = ref(null)

        const openObserve = async (project) => {
            observeProject.value = project
            observeData.value = null
            observeLoading.value = true
            showObserveModal.value = true
            try {
                const res = await fetch(`/api/projects/${project.id}`)
                observeData.value = await res.json()
            } finally {
                observeLoading.value = false
                await nextTick()
                if (treeContainer.value && observeData.value) {
                    renderJsonTree(treeContainer.value, observeData.value)
                }
            }
        }

        const treeExpandAll = () => {
            if (treeContainer.value) toggleAllNodes(treeContainer.value, true)
        }
        const treeCollapseAll = () => {
            if (treeContainer.value) toggleAllNodes(treeContainer.value, false)
        }

        // ── Boot ──
        onMounted(async () => {
            if (authUser.value && authPass.value) {
                try {
                    const res = await fetch('/api/auth', { headers: { Authorization: authHeader.value } })
                    if (res.ok) {
                        isAuthenticated.value = true
                        fetchProjects()
                        return
                    }
                } catch { /* fall through to login screen */ }
                // Stored credentials are invalid — clear them
                localStorage.removeItem('gis_auth_user')
                localStorage.removeItem('gis_auth_pass')
                authUser.value = ''
                authPass.value = ''
            }
            // Show login screen
            loading.value = false
        })

        return {
            // auth
            isAuthenticated,
            authUser,
            authHeader,
            loginForm,
            loginLoading,
            loginError,
            signIn,
            showAuthModal,
            authDraft,
            authModalError,
            authModalLoading,
            openAuthModal,
            saveCredentials,
            // projects
            projects,
            loading,
            showUploadModal,
            uploading,
            error,
            form,
            gdbInput,
            aprxInput,
            atbxInput,
            fetchProjects,
            uploadProject,
            deleteProject,
            formatDate,
            // observe
            showObserveModal,
            observeData,
            observeLoading,
            observeProject,
            treeContainer,
            openObserve,
            treeExpandAll,
            treeCollapseAll,
        }
    }
}).mount('#app')
