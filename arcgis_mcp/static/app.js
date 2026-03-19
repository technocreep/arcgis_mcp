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
        preview.textContent = expanded.value ? '' : (isArr ? `${count} items` : `${count} keys`)
        row.appendChild(preview)

        wrapper.appendChild(row)

        const children = document.createElement('div')
        children.className = 'json-children'
        children.style.display = expanded.value ? 'block' : 'none'

        childKeys.forEach((k) => {
            const v = value[k]
            const childNode = buildJsonNode(k, v, depth + 1, depth < 1)
            children.appendChild(childNode)
        })

        const closeLine = document.createElement('div')
        closeLine.className = 'text-slate-500 font-mono'
        closeLine.textContent = bracket[1]

        wrapper.appendChild(children)
        wrapper.appendChild(closeLine)

        row.addEventListener('click', () => {
            expanded.value = !expanded.value
            arrow.textContent = expanded.value ? '▼' : '▶'
            children.style.display = expanded.value ? 'block' : 'none'
            preview.textContent = expanded.value ? '' : (isArr ? `${count} items` : `${count} keys`)
        })

        wrapper.dataset.expandable = 'true'
        wrapper._toggleExpand = (forceExpand) => {
            expanded.value = forceExpand
            arrow.textContent = expanded.value ? '▼' : '▶'
            children.style.display = expanded.value ? 'block' : 'none'
            preview.textContent = expanded.value ? '' : (isArr ? `${count} items` : `${count} keys`)
        }

    } else {
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
        // ── Toast notifications ──
        const toasts = ref([])
        const addToast = (msg, type = 'info') => {
            const id = Date.now()
            toasts.value.push({ id, msg, type })
            setTimeout(() => { toasts.value = toasts.value.filter(t => t.id !== id) }, 4000)
        }
        const toastIcon = (type) => ({
            success: 'fa-solid fa-circle-check',
            error:   'fa-solid fa-circle-exclamation',
            info:    'fa-solid fa-circle-info',
            warn:    'fa-solid fa-triangle-exclamation',
        }[type] || 'fa-solid fa-circle-info')

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
                    addToast('Credentials updated.', 'success')
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
        const projectsCube = ref({})  // project_id -> boolean
        const loading = ref(true)
        const showUploadModal = ref(false)
        const uploading = ref(false)
        const uploadProgress = ref(0)   // 0-100, upload transfer progress
        const uploadPhase = ref(null)   // null | 'uploading' | 'processing'
        const error = ref(null)

        const form = ref({ id: '' })
        const gdbInput = ref(null)
        const aprxInput = ref(null)
        const atbxInput = ref(null)

        // Drag-and-drop for GDB file
        const gdbDragOver = ref(false)
        const gdbFile = ref(null)
        const onGdbDrop = (e) => {
            gdbDragOver.value = false
            const file = e.dataTransfer.files[0]
            if (file) gdbFile.value = file
        }

        const checkDatacubeStatus = async (projectId) => {
            try {
                const opts = authHeader.value ? { headers: { Authorization: authHeader.value } } : {}
                const r = await fetch(`/api/projects/${projectId}/datacube`, opts)
                if (r.ok) {
                    const d = await r.json()
                    projectsCube.value = { ...projectsCube.value, [projectId]: d.exists }
                }
            } catch { /* ignore */ }
        }

        const fetchProjects = async () => {
            loading.value = true
            try {
                const res = await fetch('/api/projects')
                const data = await res.json()
                projects.value = data.projects
                data.projects.forEach(p => checkDatacubeStatus(p.id))
            } catch (e) {
                console.error(e)
            } finally {
                loading.value = false
            }
        }

        // Upload with XHR for real progress tracking
        const uploadProject = () => {
            const gdb = gdbFile.value || gdbInput.value?.files[0]
            if (!gdb) { error.value = 'Please select a .gdb zip archive.'; return }

            uploading.value = true
            uploadProgress.value = 0
            uploadPhase.value = 'uploading'
            error.value = null

            const formData = new FormData()
            formData.append('project_id', form.value.id)
            formData.append('gdb_zip', gdb)
            if (aprxInput.value?.files[0]) formData.append('aprx', aprxInput.value.files[0])
            if (atbxInput.value?.files[0]) formData.append('atbx', atbxInput.value.files[0])

            const xhr = new XMLHttpRequest()

            xhr.upload.onprogress = (e) => {
                if (e.lengthComputable) {
                    uploadProgress.value = Math.round(e.loaded / e.total * 100)
                    if (uploadProgress.value >= 100) uploadPhase.value = 'processing'
                }
            }

            xhr.onload = async () => {
                uploading.value = false
                uploadPhase.value = null
                if (xhr.status === 401) {
                    error.value = 'Invalid credentials. Please re-authenticate.'
                } else if (xhr.status >= 200 && xhr.status < 300) {
                    showUploadModal.value = false
                    form.value.id = ''
                    gdbFile.value = null
                    if (gdbInput.value) gdbInput.value.value = ''
                    if (aprxInput.value) aprxInput.value.value = ''
                    if (atbxInput.value) atbxInput.value.value = ''
                    addToast('Project uploaded successfully.', 'success')
                    await fetchProjects()
                } else {
                    try {
                        const err = JSON.parse(xhr.responseText)
                        error.value = err.detail || 'Upload failed'
                    } catch { error.value = 'Upload failed' }
                }
            }

            xhr.onerror = () => {
                uploading.value = false
                uploadPhase.value = null
                error.value = 'Network error.'
            }

            xhr.open('POST', '/api/upload')
            if (authHeader.value) xhr.setRequestHeader('Authorization', authHeader.value)
            xhr.send(formData)
        }

        // ── Delete confirmation ──
        const deleteTarget = ref(null)
        const confirmDelete = (project) => { deleteTarget.value = project }
        const cancelDelete = () => { deleteTarget.value = null }
        const doDelete = async () => {
            const project = deleteTarget.value
            deleteTarget.value = null
            try {
                const res = await fetch(`/api/projects/${project.id}`, {
                    method: 'DELETE',
                    headers: { Authorization: authHeader.value }
                })
                if (res.status === 401) { addToast('Invalid credentials.', 'error'); return }
                if (res.ok) {
                    addToast(`"${project.name || project.id}" deleted.`, 'success')
                    await fetchProjects()
                }
            } catch { addToast('Failed to delete project.', 'error') }
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

        // ── Data Cube ──
        const DC_STAGES = ['grid', 'features', 'qa', 'labels', 'training', 'evaluation', 'visualization']
        const DC_STAGE_LABELS = {
            grid:           'Grid',
            features:       'Features',
            qa:             'QA',
            labels:         'Labels',
            training:       'Training',
            evaluation:     'Evaluation',
            visualization:  'Visualization',
        }
        const DC_STAGE_LABELS_LONG = {
            grid:           'Building grid',
            features:       'Computing features',
            qa:             'Quality assurance',
            labels:         'Assigning labels',
            training:       'Training model',
            evaluation:     'Evaluating model',
            visualization:  'Generating maps',
        }

        const showDataCubeModal = ref(false)
        const dataCubeProject = ref(null)
        const dataCubeStatus = ref(null)   // null | 'running' | 'done' | 'failed'
        const dataCubeStage = ref(null)
        const dataCubeError = ref(null)
        const dcPollInterval = ref(null)

        // Which project currently has a running/just-finished job (survives modal close)
        const activeJobProjectId = ref(null)

        // Elapsed time timer
        const dcElapsed = ref(0)
        const _dcTimer = { id: null }  // non-reactive, plain object
        const dcElapsedLabel = computed(() => {
            const m = Math.floor(dcElapsed.value / 60)
            const s = dcElapsed.value % 60
            return m > 0 ? `${m}m ${s}s` : `${s}s`
        })

        const _stopTimer = () => {
            if (_dcTimer.id) { clearInterval(_dcTimer.id); _dcTimer.id = null }
        }
        const _startTimer = () => {
            _stopTimer()
            _dcTimer.id = setInterval(() => dcElapsed.value++, 1000)
        }

        const dcDefaultForm = () => ({
            step_m: 500, pad: 0.10,
            pos_radius_m: 5000, neg_ratio: 5, ore_layer: '', seed: 42,
            model_type: 'catboost', splits: 3, group_block_m: 50000,
            rs_enabled: true, rs_reuse_existing: true,
            fault_radius_m: 10000, contact_radius_m: 10000, top_fault_classes: 10,
            fault_radii_m: '', contact_radii_m: '',
            auto_discover: true, discovery_field: '', max_auto_profiles: 6,
            geometry_mode: 'auto', include_gis_layers: false,
            run_interpretability: true,
        })

        const DC_PRESETS = {
            lekyn: {
                step_m: 500, pad: 0.10,
                pos_radius_m: 5000, neg_ratio: 5, ore_layer: 'DrudP_R_42', seed: 42,
                model_type: 'catboost', splits: 3, group_block_m: 30000,
                rs_enabled: true, rs_reuse_existing: true,
                fault_radius_m: 10000, contact_radius_m: 10000, top_fault_classes: 10,
                fault_radii_m: '', contact_radii_m: '',
                auto_discover: true, discovery_field: 'N_TYPE', max_auto_profiles: 6,
                geometry_mode: 'auto', include_gis_layers: false, run_interpretability: true,
            },
            kolpino: {
                step_m: 250, pad: 0.10,
                pos_radius_m: 2500, neg_ratio: 5, ore_layer: 'Точки_опробования_с_содержанием_Au_в_пробе', seed: 42,
                model_type: 'catboost', splits: 3, group_block_m: 25000,
                rs_enabled: true, rs_reuse_existing: true,
                fault_radius_m: 10000, contact_radius_m: 10000, top_fault_classes: 10,
                fault_radii_m: '', contact_radii_m: '',
                auto_discover: true, discovery_field: '', max_auto_profiles: 6,
                geometry_mode: 'auto', include_gis_layers: false, run_interpretability: true,
            },
        }

        const dcForm = ref(dcDefaultForm())
        const dcSelectedPreset = ref('')

        const loadDcPreset = () => {
            const preset = DC_PRESETS[dcSelectedPreset.value]
            if (preset) dcForm.value = { ...dcDefaultForm(), ...preset }
        }

        const dataCubeStageIndex = computed(() => DC_STAGES.indexOf(dataCubeStage.value))

        const dataCubeProgress = computed(() => {
            if (dataCubeStatus.value === 'done') return 100
            if (dataCubeStageIndex.value < 0) return 0
            if (dataCubeStatus.value === 'failed')
                return Math.round(((dataCubeStageIndex.value + 1) / DC_STAGES.length) * 100)
            return Math.round(((dataCubeStageIndex.value + 1) / DC_STAGES.length) * 100)
        })

        const dataCubeProgressLabel = computed(() =>
            dataCubeStage.value ? (DC_STAGE_LABELS_LONG[dataCubeStage.value] || dataCubeStage.value) : ''
        )

        const openDataCube = (project) => {
            // If this project already has an active/finished job, show its state
            if (activeJobProjectId.value === project.id && dataCubeStatus.value !== null) {
                dataCubeProject.value = project
                showDataCubeModal.value = true
                return
            }
            // Fresh open — show form
            dataCubeProject.value = project
            dataCubeStatus.value = null
            dataCubeStage.value = null
            dataCubeError.value = null
            dcForm.value = dcDefaultForm()
            dcSelectedPreset.value = ''
            showDataCubeModal.value = true
        }

        // Closing modal does NOT stop the job — polling continues in background
        const closeDataCube = () => {
            showDataCubeModal.value = false
        }

        // Force-reset when user wants to start a new job on same project
        const resetDataCube = () => {
            if (dcPollInterval.value) { clearInterval(dcPollInterval.value); dcPollInterval.value = null }
            _stopTimer()
            activeJobProjectId.value = null
            dataCubeStatus.value = null
            dataCubeStage.value = null
            dataCubeError.value = null
            dcElapsed.value = 0
        }

        const parseRadii = (str) => str
            ? str.split(',').map(s => parseFloat(s.trim())).filter(n => !isNaN(n))
            : []

        const submitDataCube = async () => {
            const projectId = dataCubeProject.value.id
            dataCubeStatus.value = 'running'
            dataCubeStage.value = 'grid'
            dataCubeError.value = null
            activeJobProjectId.value = projectId
            dcElapsed.value = 0
            _startTimer()

            const payload = {
                project_id: projectId,
                step_m: dcForm.value.step_m,
                pad: dcForm.value.pad,
                pos_radius_m: dcForm.value.pos_radius_m,
                neg_ratio: dcForm.value.neg_ratio,
                seed: dcForm.value.seed,
                model_type: dcForm.value.model_type,
                splits: dcForm.value.splits,
                group_block_m: dcForm.value.group_block_m,
                ore_layer: dcForm.value.ore_layer || null,
                rs_enabled: dcForm.value.rs_enabled,
                rs_reuse_existing: dcForm.value.rs_reuse_existing,
                fault_radius_m: dcForm.value.fault_radius_m,
                contact_radius_m: dcForm.value.contact_radius_m,
                top_fault_classes: dcForm.value.top_fault_classes,
                fault_radii_m: parseRadii(dcForm.value.fault_radii_m),
                contact_radii_m: parseRadii(dcForm.value.contact_radii_m),
                auto_discover: dcForm.value.auto_discover,
                discovery_field: dcForm.value.discovery_field || null,
                max_auto_profiles: dcForm.value.max_auto_profiles,
                geometry_mode: dcForm.value.geometry_mode,
                include_gis_layers: dcForm.value.include_gis_layers,
                run_interpretability: dcForm.value.run_interpretability,
            }

            try {
                const res = await fetch('/api/datacube/jobs', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload),
                })
                if (!res.ok) {
                    const err = await res.json().catch(() => ({}))
                    throw new Error(err.detail || 'Failed to start job')
                }
                const { job_id } = await res.json()
                // Poll every 3 s regardless of whether modal is open
                dcPollInterval.value = setInterval(() => pollDataCube(projectId, job_id), 3000)
            } catch (e) {
                dataCubeStatus.value = 'failed'
                dataCubeError.value = e.message
                _stopTimer()
            }
        }

        const pollDataCube = async (projectId, jobId) => {
            try {
                const res = await fetch(`/api/datacube/jobs/${jobId}`)
                if (!res.ok) return
                const data = await res.json()
                // Always update — even if modal is closed
                if (data.stage) dataCubeStage.value = data.stage
                if (data.status === 'done' || data.status === 'failed') {
                    dataCubeStatus.value = data.status
                    dataCubeError.value = data.error || null
                    clearInterval(dcPollInterval.value)
                    dcPollInterval.value = null
                    _stopTimer()
                    if (data.status === 'done') {
                        checkDatacubeStatus(projectId)
                        addToast('Data Cube pipeline completed.', 'success')
                    } else {
                        addToast('Data Cube pipeline failed.', 'error')
                    }
                }
            } catch { /* ignore transient network errors */ }
        }

        const openDataCubeViewer = (projectId) => {
            window.open(`/ui/datacube/?project_id=${projectId}`, '_blank')
        }

        // Project card computed helpers
        const isJobRunning = (projectId) =>
            activeJobProjectId.value === projectId && dataCubeStatus.value === 'running'

        const cubeReady = (projectId) =>
            projectsCube.value[projectId] && !isJobRunning(projectId)

        // ── Boot ──
        onMounted(async () => {
            // Escape key — close active modal (never kills DC job)
            document.addEventListener('keydown', e => {
                if (e.key !== 'Escape') return
                if (deleteTarget.value)         { cancelDelete(); return }
                if (showObserveModal.value)      { showObserveModal.value = false; return }
                if (showUploadModal.value)       { showUploadModal.value = false; return }
                if (showAuthModal.value)         { showAuthModal.value = false; return }
                if (showDataCubeModal.value)     { closeDataCube() }
            })

            if (authUser.value && authPass.value) {
                try {
                    const res = await fetch('/api/auth', { headers: { Authorization: authHeader.value } })
                    if (res.ok) {
                        isAuthenticated.value = true
                        fetchProjects()
                        return
                    }
                } catch { /* fall through */ }
                localStorage.removeItem('gis_auth_user')
                localStorage.removeItem('gis_auth_pass')
                authUser.value = ''
                authPass.value = ''
            }
            loading.value = false
        })

        return {
            // toasts
            toasts, addToast, toastIcon,
            // auth
            isAuthenticated, authUser, authHeader,
            loginForm, loginLoading, loginError, signIn,
            showAuthModal, authDraft, authModalError, authModalLoading,
            openAuthModal, saveCredentials,
            // projects
            projects, loading,
            showUploadModal, uploading, uploadProgress, uploadPhase,
            error, form,
            gdbInput, aprxInput, atbxInput,
            gdbDragOver, gdbFile, onGdbDrop,
            fetchProjects, uploadProject,
            // delete
            deleteTarget, confirmDelete, cancelDelete, doDelete,
            formatDate,
            // observe
            showObserveModal, observeData, observeLoading, observeProject,
            treeContainer, openObserve, treeExpandAll, treeCollapseAll,
            // data cube
            DC_STAGES, DC_STAGE_LABELS, DC_STAGE_LABELS_LONG,
            showDataCubeModal, dataCubeProject,
            dataCubeStatus, dataCubeStage, dataCubeStageIndex,
            dataCubeProgress, dataCubeProgressLabel,
            dataCubeError, dcElapsed, dcElapsedLabel,
            dcForm, dcSelectedPreset, loadDcPreset,
            activeJobProjectId,
            openDataCube, closeDataCube, resetDataCube,
            submitDataCube, openDataCubeViewer,
            projectsCube, isJobRunning, cubeReady,
        }
    }
}).mount('#app')
