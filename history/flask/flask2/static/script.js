let allData = [];   // store all fetched rows
let currentPage = 1;
let startDate = '';
let endDate = '';
const perPage = 50;
let headersSet = false; // track if table headers are already added

document.addEventListener('DOMContentLoaded', function () {
    document.getElementById('dateForm').addEventListener('submit', function (e) {
        e.preventDefault();
        startDate = document.getElementById('start').value;
        endDate = document.getElementById('end').value;
        currentPage = 1;
        allData = [];
        document.getElementById('tableBody').innerHTML = '';
        headersSet = false;
        loadAllData();
        document.getElementById('downloadBtn').style.display = 'inline-block';
        document.getElementById('nextBtn').style.display = 'inline-block';
    });

    document.getElementById('nextBtn').addEventListener('click', function () {
        if ((currentPage * perPage) >= allData.length) {
            alert('No more data');
            return;
        }
        currentPage++;
        displayPage();
    });

    document.getElementById('downloadBtn').addEventListener('click', function () {
        window.location.href = `/download?start=${startDate}&end=${endDate}`;
    });
});

function loadAllData() {
    fetch(`/data?start=${startDate}&end=${endDate}&page=1`)
        .then(response => response.json())
        .then(data => {
            if (data.length === 0) {
                alert('No data found for this date range');
                return;
            }

            allData = data; // store first page
            fetchNextPage(2); // recursively fetch additional pages
        });
}

function fetchNextPage(pageNum) {
    fetch(`/data?start=${startDate}&end=${endDate}&page=${pageNum}`)
        .then(r => r.json())
        .then(nextData => {
            if (nextData.length > 0) {
                allData = allData.concat(nextData);
                fetchNextPage(pageNum + 1);
            } else {
                displayPage(); // display first page after all fetched
            }
        });
}

function displayPage() {
    let startIdx = (currentPage - 1) * perPage;
    let endIdx = startIdx + perPage;
    let pageData = allData.slice(startIdx, endIdx);

    let headerRow = document.getElementById('tableHeader');
    if (!headersSet && pageData.length > 0) {
        headerRow.innerHTML = '';
        Object.keys(pageData[0]).forEach(col => {
            let th = document.createElement('th');
            th.innerText = col;
            headerRow.appendChild(th);
        });
        headersSet = true; // only set headers once
    }

    let body = document.getElementById('tableBody');
    // clear table only if first page
    if (currentPage === 1) body.innerHTML = '';

    pageData.forEach(row => {
        let tr = document.createElement('tr');
        Object.values(row).forEach(value => {
            let td = document.createElement('td');
            td.innerText = value === null ? '' : value;
            tr.appendChild(td);
        });
        body.appendChild(tr);
    });
}
