let currentPage = 1;
let startDate = '';
let endDate = '';

document.addEventListener('DOMContentLoaded', function () {
    document.getElementById('dateForm').addEventListener('submit', function (e) {
        e.preventDefault();
        startDate = document.getElementById('start').value;
        endDate = document.getElementById('end').value;
        currentPage = 1;
        document.getElementById('tableBody').innerHTML = '';
        loadPage();
        document.getElementById('downloadBtn').style.display = 'inline-block';
        document.getElementById('nextBtn').style.display = 'inline-block';
    });

    document.getElementById('nextBtn').addEventListener('click', function () {
        currentPage++;
        loadPage();
    });

    document.getElementById('downloadBtn').addEventListener('click', function () {
        window.location.href = `/download?start=${startDate}&end=${endDate}`;
    });
});

function loadPage() {
    fetch(`/data?start=${startDate}&end=${endDate}&page=${currentPage}`)
        .then(response => response.json())
        .then(data => {
            if (data.length === 0) {
                alert('No more data');
                return;
            }

            if (currentPage === 1) {
                let headerRow = document.getElementById('tableHeader');
                headerRow.innerHTML = '';
                Object.keys(data[0]).forEach(col => {
                    let th = document.createElement('th');
                    th.innerText = col;
                    headerRow.appendChild(th);
                });
            }

            let body = document.getElementById('tableBody');
            data.forEach(row => {
                let tr = document.createElement('tr');
                Object.values(row).forEach(value => {
                    let td = document.createElement('td');
                    td.innerText = value === null ? '' : value;
                    tr.appendChild(td);
                });
                body.appendChild(tr);
            });
        });
}

