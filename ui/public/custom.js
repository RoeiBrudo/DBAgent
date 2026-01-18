// Custom UI for DBAgent - Simple toolbar

(function() {
    let sqlMode = false;
    let spiderDbs = [];
    let birdDbs = [];
    let dbsLoaded = false;
    let stateLoaded = false;

    function createToolbar() {
        if (document.getElementById('dbagent-toolbar')) return;

        const toolbar = document.createElement('div');
        toolbar.id = 'dbagent-toolbar';
        toolbar.innerHTML = `
            <div class="dba-container">
                <div class="dba-title">
                    <span class="dba-logo">🤖</span>
                    <span class="dba-name">DBAgent</span>
                    <span class="dba-desc">Text-to-SQL Assistant</span>
                </div>
                <div class="dba-dropdown">
                    <select id="dba-dataset" class="dba-select">
                        <option value="">📁 Select Dataset</option>
                        <option value="spider">🕷️ Spider</option>
                        <option value="bird">🐦 Bird</option>
                        <option value="custom">📤 Upload Custom</option>
                    </select>
                </div>
                <div class="dba-dropdown" id="dba-db-container" style="display:none;">
                    <select id="dba-database" class="dba-select">
                        <option value="">Select Database...</option>
                    </select>
                </div>
                <button id="dba-upload-btn" class="dba-upload-btn" style="display:none;">📤 Upload File</button>
                <input id="dba-upload-input" type="file" accept=".db,.sqlite,application/x-sqlite3" style="display:none;" />
                <div class="dba-status">
                    <span id="dba-current">No database selected</span>
                </div>
                <div class="dba-mode">
                    <span class="dba-mode-label" id="dba-text-label">Text</span>
                    <label class="dba-toggle">
                        <input type="checkbox" id="dba-sql-toggle">
                        <span class="dba-toggle-slider"></span>
                    </label>
                    <span class="dba-mode-label" id="dba-sql-label">SQL</span>
                </div>
            </div>
        `;
        document.body.insertBefore(toolbar, document.body.firstChild);

        // Add padding to main content
        setTimeout(() => {
            const main = document.querySelector('main');
            if (main) main.style.paddingTop = '60px';
        }, 100);

        setupListeners();
        updateModeDisplay();

        // Load initial datasets + state
        setTimeout(() => {
            loadDatasets();
            loadState();
        }, 200);
    }

    function setupListeners() {
        // Dataset selector
        document.getElementById('dba-dataset').addEventListener('change', async (e) => {
            const val = e.target.value;
            const uploadBtn = document.getElementById('dba-upload-btn');
            const dbContainer = document.getElementById('dba-db-container');
            
            uploadBtn.style.display = 'none';
            dbContainer.style.display = 'none';
            
            if (val === 'custom') {
                uploadBtn.style.display = 'block';
            } else if (val === 'spider') {
                dbContainer.style.display = 'block';
                if (!dbsLoaded) await loadDatasets();
                populateDbDropdown(spiderDbs);
            } else if (val === 'bird') {
                dbContainer.style.display = 'block';
                if (!dbsLoaded) await loadDatasets();
                populateDbDropdown(birdDbs);
            }
        });

        // Database selector
        document.getElementById('dba-database').addEventListener('change', (e) => {
            const db = e.target.value;
            const dataset = document.getElementById('dba-dataset').value;
            if (db && (dataset === 'spider' || dataset === 'bird')) {
                switchDb(dataset, db);
            }
        });

        // Upload button
        document.getElementById('dba-upload-btn').addEventListener('click', () => {
            const input = document.getElementById('dba-upload-input');
            if (input) input.click();
        });

        document.getElementById('dba-upload-input').addEventListener('change', async (e) => {
            const file = e.target.files && e.target.files[0];
            if (!file) return;
            await uploadDb(file);
            // reset input so same file can be uploaded again
            e.target.value = '';
        });

        // SQL mode toggle
        document.getElementById('dba-sql-toggle').addEventListener('change', (e) => {
            sqlMode = e.target.checked;
            updateModeDisplay();
            setSqlMode(sqlMode);
        });
    }

    function updateModeDisplay() {
        const textLabel = document.getElementById('dba-text-label');
        const sqlLabel = document.getElementById('dba-sql-label');
        if (sqlMode) {
            textLabel.style.opacity = '0.5';
            sqlLabel.style.opacity = '1';
            sqlLabel.style.fontWeight = 'bold';
            textLabel.style.fontWeight = 'normal';
        } else {
            textLabel.style.opacity = '1';
            sqlLabel.style.opacity = '0.5';
            textLabel.style.fontWeight = 'bold';
            sqlLabel.style.fontWeight = 'normal';
        }
    }

    async function fetchJsonWithRetry(url, options, retries = 6, delayMs = 250) {
        let lastErr = null;
        for (let i = 0; i < retries; i++) {
            try {
                const res = await fetch(url, { ...(options || {}), credentials: 'include' });
                if (res.status === 401) {
                    // Session cookie might not be set yet; retry
                    await new Promise(r => setTimeout(r, delayMs));
                    continue;
                }
                const isJson = (res.headers.get('content-type') || '').includes('application/json');
                const body = isJson ? await res.json() : null;
                return { ok: res.ok, status: res.status, body };
            } catch (e) {
                lastErr = e;
                await new Promise(r => setTimeout(r, delayMs));
            }
        }
        throw lastErr || new Error('Request failed');
    }

    async function loadDatasets() {
        try {
            const res = await fetchJsonWithRetry('/api/dbagent/datasets', undefined, 4, 200);
            if (!res.ok) return;
            const data = res.body || {};
            spiderDbs = Array.isArray(data.spider) ? data.spider : [];
            birdDbs = Array.isArray(data.bird) ? data.bird : [];
            dbsLoaded = true;

            const dataset = document.getElementById('dba-dataset')?.value;
            if (dataset === 'spider') populateDbDropdown(spiderDbs);
            if (dataset === 'bird') populateDbDropdown(birdDbs);
        } catch (_) {
            // ignore
        }
    }

    async function loadState() {
        try {
            const res = await fetchJsonWithRetry('/api/dbagent/state');
            if (!res.ok) return;
            const data = res.body || {};
            stateLoaded = true;
            if (typeof data.sql_mode === 'boolean') {
                sqlMode = data.sql_mode;
                const toggle = document.getElementById('dba-sql-toggle');
                if (toggle) toggle.checked = sqlMode;
                updateModeDisplay();
            }
            if (data.db_name) {
                document.getElementById('dba-current').textContent = data.db_name;
            }
        } catch (_) {
            // ignore
        }
    }

    async function switchDb(dataset, db) {
        // optimistic UI update
        document.getElementById('dba-current').textContent = `${dataset}/${db}`;
        try {
            const res = await fetchJsonWithRetry('/api/dbagent/switch-db', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ dataset, db })
            });
            if (!res.ok) {
                // revert on error
                await loadState();
            }
        } catch (_) {
            await loadState();
        }
    }

    async function setSqlMode(enabled) {
        try {
            await fetchJsonWithRetry('/api/dbagent/sql-mode', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ enabled })
            });
        } catch (_) {
            // ignore
        }
    }

    async function uploadDb(file) {
        // optimistic UI update
        document.getElementById('dba-current').textContent = `uploaded/${file.name}`;
        try {
            const form = new FormData();
            form.append('file', file, file.name);
            const res = await fetchJsonWithRetry('/api/dbagent/upload-db', {
                method: 'POST',
                body: form,
            });
            if (!res.ok) {
                await loadState();
                return;
            }
            const data = res.body;
            if (data && data.db_name) {
                document.getElementById('dba-current').textContent = data.db_name;
            }
        } catch (_) {
            await loadState();
        }
    }

    function populateDbDropdown(dbs) {
        const select = document.getElementById('dba-database');
        if (!select) return;

        // Reset
        select.innerHTML = '<option value="">Select Database...</option>';

        if (!dbsLoaded) {
            const opt = document.createElement('option');
            opt.value = '';
            opt.textContent = 'Loading...';
            select.appendChild(opt);
            return;
        }

        (dbs || []).forEach((db) => {
            const opt = document.createElement('option');
            opt.value = db;
            opt.textContent = db;
            select.appendChild(opt);
        });
    }

    function watchMessages() {
        // No-op: toolbar no longer relies on parsing chat messages
    }

    // Initialize
    function init() {
        createToolbar();
        watchMessages();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        setTimeout(init, 500);
    }
})();
