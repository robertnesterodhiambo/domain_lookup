let currentPage = 1;
let startDate = '';
let endDate = '';
const perPage = 50;
let filters = {};
let headersSet = false;

let selectedColumns = [];   // user-selected columns
let allColumns = [];        // from backend /columns

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

            // (Optional) Preselect a few useful columns
            ['domain_name', 'create_date', 'expiry_date'].forEach(c => {
                const op = [...sel.options].find(o => o.value === c);
                if (op) op.selected = true;
            });
        })
        .catch(() => {
            // If it fails, backend will default to all columns anyway.
        });

    document.getElementById('dateForm').addEventListener('submit', function (e) {
        e.preventDefault();
        startDate = document.getElementById('start').value;
        endDate = document.getElementById('end').value;

        selectedColumns = getSelectedColumns();

        currentPage = 1;
        filters = {};
        document.getElementById('tableBody').innerHTML = '';
        headersSet = false;
        document.getElementById('tableHeader').innerHTML = '';
        loadPage(currentPage);

        document.getElementById('downloadBtn').style.display = 'inline-block';
        document.getElementById('nextBtn').style.display = 'inline-block';
    });

    document.getElementById('nextBtn').addEventListener('click', function () {
        currentPage++;
        loadPage(currentPage);
    });

    document.getElementById('downloadBtn').addEventListener('click', function () {
        const colsParam = buildColumnsParam();
        // Download full data directly from the server (streamed)
        window.location.href = `/download?start=${encodeURIComponent(startDate)}&end=${encodeURIComponent(endDate)}${colsParam}`;
    });

    // If user changes column selection, reload (if dates already chosen)
    document.getElementById('columns').addEventListener('change', function () {
        selectedColumns = getSelectedColumns();
        if (startDate && endDate) {
            currentPage = 1;
            filters = {};
            headersSet = false;
            document.getElementById('tableHeader').innerHTML = '';
            document.getElementById('tableBody').innerHTML = '';
            loadPage(currentPage);
        }
    });
});

function getSelectedColumns() {
    const select = document.getElementById('columns');
    return Array.from(select.selectedOptions).map(opt => opt.value);
}

function buildColumnsParam() {
    return (selectedColumns && selectedColumns.length)
        ? `&columns=${encodeURIComponent(selectedColumns.join(','))}`
        : '';
}

function loadPage(page) {
    let query = `/data?start=${encodeURIComponent(startDate)}&end=${encodeURIComponent(endDate)}&page=${page}${buildColumnsParam()}`;
    fetch(query)
        .then(response => response.json())
        .then(data => {
            if (!Array.isArray(data)) {
                alert("Error loading data");
                return;
            }

            if (data.length === 0) {
                if (page === 1) {
                    alert("No data for this date range");
                } else {
                    alert("No more pages");
                    currentPage--; // prevent going past last page
                }
                return;
            }

            // Decide header order:
            // - if user selected columns, honor that order
            // - otherwise, use keys from the first row
            let keys = (selectedColumns && selectedColumns.length)
                ? selectedColumns.slice()
                : Object.keys(data[0]);

            // Ensure 'domain_name' first if present
            if (keys.includes('domain_name')) {
                keys = ['domain_name', ...keys.filter(k => k !== 'domain_name')];
            }

            // Build headers once (with per-column filter inputs)
            if (!headersSet) {
                const headerRow = document.getElementById('tableHeader');
                headerRow.innerHTML = '';

                keys.forEach(col => {
                    const th = document.createElement('th');
                    th.innerText = col;

                    const input = document.createElement('input');
                    input.type = 'text';
                    input.placeholder = `Filter ${col}`;
                    input.style.width = '90%';
                    input.addEventListener('input', function () {
                        filters[col] = this.value.toLowerCase();
                        currentPage = 1;
                        document.getElementById('tableBody').innerHTML = '';
                        loadPage(currentPage);
                    });

                    th.appendChild(document.createElement('br'));
                    th.appendChild(input);
                    headerRow.appendChild(th);
                });
                headersSet = true;
            }

            // Apply client-side filtering for the current page
            const filteredData = data.filter(row => {
                return Object.keys(filters).every(col => {
                    if (!filters[col]) return true;
                    const val = row[col] ? row[col].toString().toLowerCase() : '';
                    return val.includes(filters[col]);
                });
            });

            // Append rows
            const body = document.getElementById('tableBody');
            filteredData.forEach(row => {
                const tr = document.createElement('tr');
                let rowKeys = keys.slice();
                rowKeys.forEach(col => {
                    const td = document.createElement('td');
                    td.innerText = (row[col] == null) ? '' : row[col];
                    tr.appendChild(td);
                });
                body.appendChild(tr);
            });
        })
        .catch(() => {
            alert("Failed to fetch data");
        });
}
