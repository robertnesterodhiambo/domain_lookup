let allData = [];
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
        allData = [];
        document.getElementById('tableBody').innerHTML = '';
        filters = {};
        headersSet = false;
        loadAllData();

        document.getElementById('downloadBtn').style.display = 'inline-block';
        document.getElementById('nextBtn').style.display = 'inline-block';
    });

    document.getElementById('nextBtn').addEventListener('click', function () {
        const totalPages = Math.ceil(filteredData().length / perPage);
        if (currentPage < totalPages) {
            currentPage++;
            displayPage();
        } else {
            alert('No more data');
        }
    });

    document.getElementById('downloadBtn').addEventListener('click', function () {
        // download current filtered data
        let csvContent = "data:text/csv;charset=utf-8,";

        const rows = filteredData();
        if (rows.length === 0) return alert("No data to download");

        let keys = Object.keys(rows[0]);
        if (keys.includes('domain_name')) {
            keys = ['domain_name', ...keys.filter(k => k !== 'domain_name')];
        }

        csvContent += keys.join(",") + "\n";

        rows.forEach(row => {
            csvContent += keys.map(k => row[k] ?? '').join(",") + "\n";
        });

        const encodedUri = encodeURI(csvContent);
        const link = document.createElement("a");
        link.setAttribute("href", encodedUri);
        link.setAttribute("download", "data.csv");
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    });
});

function loadAllData() {
    // fetch all pages until empty
    function fetchPage(page) {
        fetch(`/data?start=${startDate}&end=${endDate}&page=${page}`)
            .then(r => r.json())
            .then(data => {
                if (data.length > 0) {
                    allData = allData.concat(data);
                    fetchPage(page + 1);
                } else {
                    displayPage();
                }
            });
    }
    fetchPage(1);
}

function filteredData() {
    return allData.filter(row => {
        return Object.keys(filters).every(col => {
            if (!filters[col]) return true;
            const val = row[col] ? row[col].toString().toLowerCase() : '';
            return val.includes(filters[col].toLowerCase());
        });
    });
}

function displayPage() {
    const pageData = filteredData().slice((currentPage - 1) * perPage, currentPage * perPage);

    const headerRow = document.getElementById('tableHeader');
    if (!headersSet && pageData.length > 0) {
        headerRow.innerHTML = '';
        let keys = Object.keys(pageData[0]);
        if (keys.includes('domain_name')) keys = ['domain_name', ...keys.filter(k => k !== 'domain_name')];

        keys.forEach(col => {
            const th = document.createElement('th');
            th.innerText = col;

            const input = document.createElement('input');
            input.type = 'text';
            input.placeholder = `Filter ${col}`;
            input.style.width = '90%';
            input.addEventListener('input', function () {
                filters[col] = this.value;
                currentPage = 1;
                displayPage();
            });

            th.appendChild(document.createElement('br'));
            th.appendChild(input);
            headerRow.appendChild(th);
        });
        headersSet = true;
    }

    const body = document.getElementById('tableBody');
    body.innerHTML = ''; // clear previous rows

    pageData.forEach(row => {
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
}
