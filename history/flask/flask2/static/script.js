let currentPage = 1;
let startDate = '';
let endDate = '';
const perPage = 50;
let filters = {};
let headersSet = false;

document.addEventListener('DOMContentLoaded', function () {
    document.getElementById('dateForm').addEventListener('submit', function (e) {
        e.preventDefault();
        startDate = document.getElementById('start').value;
        endDate = document.getElementById('end').value;
        currentPage = 1;
        filters = {};
        document.getElementById('tableBody').innerHTML = '';
        headersSet = false;
        loadPage(currentPage);

        document.getElementById('downloadBtn').style.display = 'inline-block';
        document.getElementById('nextBtn').style.display = 'inline-block';
    });

    document.getElementById('nextBtn').addEventListener('click', function () {
        currentPage++;
        loadPage(currentPage);
    });

    document.getElementById('downloadBtn').addEventListener('click', function () {
        // Download full data directly from the server (streamed)
        window.location.href = `/download?start=${startDate}&end=${endDate}`;
    });
});

function loadPage(page) {
    let query = `/data?start=${startDate}&end=${endDate}&page=${page}`;
    fetch(query)
        .then(response => response.json())
        .then(data => {
            if (data.length === 0) {
                if (page === 1) {
                    alert("No data for this date range");
                } else {
                    alert("No more pages");
                    currentPage--; // prevent going past last page
                }
                return;
            }

            // Set headers once
            if (!headersSet) {
                const headerRow = document.getElementById('tableHeader');
                headerRow.innerHTML = '';
                let keys = Object.keys(data[0]);
                if (keys.includes('domain_name')) keys = ['domain_name', ...keys.filter(k => k !== 'domain_name')];

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

            // Filter client-side only for this page
            const filteredData = data.filter(row => {
                return Object.keys(filters).every(col => {
                    if (!filters[col]) return true;
                    const val = row[col] ? row[col].toString().toLowerCase() : '';
                    return val.includes(filters[col]);
                });
            });

            // Append rows to table
            const body = document.getElementById('tableBody');
            filteredData.forEach(row => {
                const tr = document.createElement('tr');
                let keys = Object.keys(row);
                if (keys.includes('domain_name')) keys = ['domain_name', ...keys.filter(k => k !== 'domain_name')];
                keys.forEach(col => {
                    const td = document.createElement('td');
                    td.innerText = row[col] ?? '';
                    tr.appendChild(td);
                });
                body.appendChild(tr);
            });
        });
}

