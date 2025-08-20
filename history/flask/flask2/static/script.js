// static/script.js

let allData = [];            // All rows received from server (grows during streaming)
let filteredData = [];       // allData after applying filters
let currentPage = 1;
const perPage = 50;

let startDate = '';
let endDate = '';
let selectedColumns = [];    // user-selected columns (empty means all)
let allColumns = [];         // list from backend
let headersSet = false;
let filters = {};            // { columnName: filterValue }

document.addEventListener('DOMContentLoaded', function () {
    // Populate column selector from backend fixed whitelist
    fetch('/columns')
        .then(r => r.json())
        .then(cols => {
            allColumns = Array.isArray(cols) ? cols : [];
            const sel = document.getElementById('columns');
            sel.innerHTML = '';
            allColumns.forEach(c => {
                const opt = document.createElement('option');
                opt.value = c;
                opt.textContent = c;
                sel.appendChild(opt);
            });

            // Preselect a few useful columns if they exist
            ['domain_name', 'create_date', 'expiry_date'].forEach(c => {
                const op = [...sel.options].find(o => o.value === c);
                if (op) op.selected = true;
            });

            // sync initial selectedColumns
            selectedColumns = getSelectedColumns();
        })
        .catch(() => {
            // If it fails, backend will default to all columns anyway.
        });

    document.getElementById('dateForm').addEventListener('submit', function (e) {
        e.preventDefault();

        // reset data structures & UI
        startDate = document.getElementById('start').value;
        endDate = document.getElementById('end').value;
        selectedColumns = getSelectedColumns();

        allData = [];
        filteredData = [];
        filters = {};
        currentPage = 1;
        headersSet = false;
        document.getElementById('tableHeader').innerHTML = '';
        document.getElementById('tableBody').innerHTML = '';

        // show buttons
        document.getElementById('downloadBtn').style.display = 'inline-block';
        document.getElementById('nextBtn').style.display = 'inline-block';

        // start streaming
        streamData();
    });

    document.getElementById('nextBtn').addEventListener('click', function () {
        // Show next page if exists
        const totalPages = Math.max(1, Math.ceil(filteredData.length / perPage));
        if (currentPage < totalPages) {
            currentPage++;
            renderTable();
        } else {
            // If data is still streaming, allow waiting for more rows; otherwise notify
            if (!isStreaming) {
                alert("No more pages");
            } else {
                // If streaming, user might want to wait for more rows to arrive.
                // We won't block; just notify.
                alert("No more pages yet — more data is still streaming in.");
            }
        }
    });

    document.getElementById('downloadBtn').addEventListener('click', function () {
        downloadFilteredCSV();
    });

    document.getElementById('columns').addEventListener('change', function () {
        // When the selected columns change, recompute headers & rerender using the columns user wants
        selectedColumns = getSelectedColumns();

        // Reset headers so they will be rebuilt for the chosen columns
        headersSet = false;
        document.getElementById('tableHeader').innerHTML = '';
        renderTable(); // render with updated columns
    });
});

function getSelectedColumns() {
    const select = document.getElementById('columns');
    const chosen = Array.from(select.selectedOptions).map(opt => opt.value);
    // If user selects none, return empty array (means server will default to all columns).
    // For client display convenience, we'll fall back to allColumns when rendering.
    return chosen;
}

function buildColumnsParam() {
    return (selectedColumns && selectedColumns.length)
        ? `&columns=${encodeURIComponent(selectedColumns.join(','))}`
        : '';
}

let controller = null;
let isStreaming = false;

async function streamData() {
    if (!startDate || !endDate) {
        alert("Select start and end dates");
        return;
    }

    const colsParam = buildColumnsParam();
    const url = `/data?start=${encodeURIComponent(startDate)}&end=${encodeURIComponent(endDate)}${colsParam}`;

    // abort previous controller if any
    if (controller) {
        try { controller.abort(); } catch (e) {}
    }
    controller = new AbortController();

    try {
        isStreaming = true;
        const response = await fetch(url, { signal: controller.signal });
        if (!response.ok) {
            const text = await response.text();
            alert("Server error: " + text);
            isStreaming = false;
            return;
        }

        const reader = response.body.getReader();
        const decoder = new TextDecoder();
        let { value: chunk, done: readerDone } = await reader.read();
        let buffer = chunk ? decoder.decode(chunk, { stream: true }) : '';

        // We will treat each line as a JSON object (NDJSON)
        // Keep partial line in 'buffer' and split on '\n'
        const processBuffer = (final = false) => {
            let lines = buffer.split(/\r?\n/);
            // if not final, last entry may be partial; keep it in buffer
            if (!final) {
                buffer = lines.pop();
            } else {
                buffer = '';
            }

            for (const line of lines) {
                if (!line.trim()) continue;
                try {
                    const obj = JSON.parse(line);
                    handleIncomingRow(obj);
                } catch (err) {
                    // If a single line fails to parse, skip it (but log to console)
                    console.warn("Failed to parse line:", line, err);
                }
            }
        };

        // initial chunk processed above
        processBuffer(false);

        // Continue reading stream
        while (true) {
            const readResult = await reader.read();
            chunk = readResult.value;
            readerDone = readResult.done;
            if (chunk) {
                buffer += decoder.decode(chunk, { stream: true });
                processBuffer(false);
            }
            if (readerDone) {
                // process any remaining buffered content
                if (buffer.length > 0) processBuffer(true);
                break;
            }
        }

        // streaming finished
        isStreaming = false;
        // final render in case nothing arrived or to update counts
        applyFiltersAndRender();
    } catch (err) {
        if (err.name === 'AbortError') {
            console.log('Streaming aborted by user.');
        } else {
            console.error('Streaming failed', err);
            alert('Streaming failed: ' + (err.message || err));
        }
        isStreaming = false;
    }
}

function handleIncomingRow(row) {
    // Add row to main dataset
    allData.push(row);

    // If headers haven't been built yet, build them using selectedColumns or row keys
    if (!headersSet) {
        buildHeaders(row);
    }

    // If no filters active, append to filteredData and append to DOM if within current page.
    // Otherwise re-apply filters to whole dataset (cheaper to re-filter as data streams for correctness).
    if (Object.keys(filters).length === 0) {
        filteredData.push(row);
    } else {
        // Apply current filters only to this row; if it matches, push to filteredData
        if (rowMatchesFilters(row)) filteredData.push(row);
    }

    // If the new row falls within the currently visible page, append to the table immediately
    const startIdx = (currentPage - 1) * perPage;
    const endIdx = startIdx + perPage;
    const newIndex = filteredData.length - 1;
    if (newIndex >= startIdx && newIndex < endIdx) {
        appendRowToTable(row);
    }

    // Optionally update a small indicator (not in UI since you asked not to change HTML).
    // If you want a running count, we could insert it into header or console.log
    // console.log(`Received rows: ${allData.length}, Filtered: ${filteredData.length}`);
}

function buildHeaders(sampleRow) {
    const headerRow = document.getElementById('tableHeader');
    headerRow.innerHTML = '';
    headersSet = true;

    // Determine keys/order:
    let keys;
    if (selectedColumns && selectedColumns.length) {
        // If user selected columns explicitly, use that order
        keys = selectedColumns.slice();
    } else if (sampleRow) {
        // fallback to sampleRow keys order
        keys = Object.keys(sampleRow);
    } else {
        keys = allColumns.slice();
    }

    // Prefer domain_name first if present:
    if (keys.includes('domain_name')) {
        keys = ['domain_name', ...keys.filter(k => k !== 'domain_name')];
    }

    // Persist keys for rendering
    window._renderKeys = keys;

    // Create headers with filter inputs
    keys.forEach(col => {
        const th = document.createElement('th');
        const title = document.createElement('div');
        title.innerText = col;
        th.appendChild(title);

        const input = document.createElement('input');
        input.type = 'text';
        input.placeholder = `Filter ${col}`;
        input.style.width = '90%';
        input.addEventListener('input', function () {
            const val = this.value.trim().toLowerCase();
            if (val) {
                filters[col] = val;
            } else {
                delete filters[col];
            }
            // Recompute filteredData across allData (since filter should be global)
            applyFiltersAndRender();
        });

        th.appendChild(document.createElement('br'));
        th.appendChild(input);
        headerRow.appendChild(th);
    });
}

function rowMatchesFilters(row) {
    return Object.keys(filters).every(col => {
        if (!filters[col]) return true;
        const val = row[col] ? String(row[col]).toLowerCase() : '';
        return val.includes(filters[col]);
    });
}

function applyFiltersAndRender() {
    // Recompute filteredData from allData using filters
    if (Object.keys(filters).length === 0) {
        filteredData = allData.slice();
    } else {
        filteredData = allData.filter(rowMatchesFilters);
    }

    // Reset to first page when filters change
    currentPage = 1;
    renderTable();
}

function renderTable() {
    const body = document.getElementById('tableBody');
    body.innerHTML = '';

    // Ensure headers exist
    if (!headersSet) {
        // If no sample row yet, we can't build headers. Wait for streaming to provide rows.
        // But we can build headers from selectedColumns/allColumns if available
        buildHeaders(null);
    }

    const keys = window._renderKeys || (selectedColumns.length ? selectedColumns : allColumns);

    // If there are no columns selected and allColumns empty, we still need something
    const effectiveKeys = (keys && keys.length) ? keys : allColumns;

    const totalPages = Math.max(1, Math.ceil(filteredData.length / perPage));
    if (currentPage > totalPages) currentPage = totalPages;

    const startIdx = (currentPage - 1) * perPage;
    const endIdx = Math.min(startIdx + perPage, filteredData.length);

    for (let i = startIdx; i < endIdx; i++) {
        const row = filteredData[i];
        appendRowToTable(row, effectiveKeys);
    }
}

function appendRowToTable(row, keysOverride) {
    const body = document.getElementById('tableBody');
    const tr = document.createElement('tr');

    const keys = keysOverride || window._renderKeys || (selectedColumns.length ? selectedColumns : allColumns);

    const effectiveKeys = (keys && keys.length) ? keys : allColumns;

    effectiveKeys.forEach(col => {
        const td = document.createElement('td');
        td.innerText = (row && row[col] != null) ? row[col] : '';
        tr.appendChild(td);
    });
    body.appendChild(tr);
}

function downloadFilteredCSV() {
    if (!filteredData || filteredData.length === 0) {
        alert("No rows to download (filtered result is empty).");
        return;
    }

    // Use header order equal to current visible columns (window._renderKeys)
    const keys = window._renderKeys && window._renderKeys.length ? window._renderKeys : (selectedColumns.length ? selectedColumns : allColumns);

    const safeKeys = keys && keys.length ? keys : allColumns;
    // CSV header
    let csv = safeKeys.join(',') + '\n';

    // CSV rows - escape quotes by doubling
    filteredData.forEach(row => {
        const line = safeKeys.map(k => {
            let v = row && row[k] != null ? String(row[k]) : '';
            // Replace newlines with spaces to keep CSV structure stable
            v = v.replace(/\r?\n/g, ' ');
            // If contains comma/quote/newline, wrap in quotes and escape quotes by doubling
            if (v.includes('"')) v = v.replace(/"/g, '""');
            if (v.includes(',') || v.includes('"') || v.includes('\n')) {
                return `"${v}"`;
            }
            return v;
        }).join(',');
        csv += line + '\n';
    });

    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'filtered_data.csv';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
}
