let currentData = [];
let currentMetric = 'net_mass_kg';
let currentView = 'chart';
let chartInstance = null;
let mapInstance = null;
let geojsonData = null;
let sortState = { column: null, direction: 'asc' };

const metricLabels = {
    net_mass_kg: 'Volume (Kg)',
    statistical_value_gbp: 'Value (£)'
};

async function fetchData(datasetFile) {
    try {
        // CRITICAL FIX 1: Path must go up one level (../) (Resolves 404 Error)
        const response = await fetch(`../Data/Imports/${datasetFile}.json`);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}. Could not find ${datasetFile}.json at ../Data/Imports/`);
        }
        const jsonData = await response.json();

        // CRITICAL FIX 2: Access the 'imports' array specifically. 
        // This resolves the 'data.sort is not a function' error.
        if (jsonData.imports && Array.isArray(jsonData.imports)) {
            currentData = jsonData.imports;
        } else {
                throw new Error("Data format is incorrect. Expected an 'imports' array.");
        }
        
        // Pre-process data for consistency
        currentData.forEach(item => {
            item.country_display = item.country_of_origin || item.country;
        });

        const meta = jsonData.metadata;

        // Update Titles and Metadata
        let title = 'Data Dashboard';
        if (meta) {
            if (meta.report_title) {
                title = meta.report_title;
            } else if (meta.month && meta.year) {
                const monthNames = ["", "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
                title = `UK Imports - ${monthNames[meta.month]} ${meta.year}`;
            }
        }
        document.getElementById('dashboard-title').textContent = title;
        
        const metaText = meta ? `Source: ${meta.source} | Published: ${meta.date_published || 'N/A'}` : '';
        document.getElementById('dashboard-meta').textContent = metaText;

        // Load GeoJSON data (CRITICAL FIX 1: Path ../)
        if (!geojsonData) {
            const geoResponse = await fetch('../countries.geojson');
            if (geoResponse.ok) {
                    geojsonData = await geoResponse.json();
            }
        }

        updateView();
    } catch (error) {
        console.error('Error fetching data:', error);
        document.getElementById('dashboard-title').textContent = 'Error loading dataset.';
        document.getElementById('dashboard-meta').textContent = `Error: ${error.message}`;
    }
}

function updateView() {
    document.querySelectorAll('.view-content').forEach(el => el.style.display = 'none');
    document.getElementById(`${currentView}-view`).style.display = 'block';

    if (currentView === 'chart') {
        renderChart();
    } else if (currentView === 'table') {
        if (!sortState.column) {
                document.querySelectorAll('.sort-arrow').forEach(el => el.remove());
                document.querySelectorAll('th').forEach(el => el.classList.remove('sort-asc', 'sort-desc'));
                renderTable(currentData);
        } else {
                const sortedData = sortData(currentData, sortState.column, sortState.direction);
                renderTable(sortedData);
        }
        
    } else if (currentView === 'map') {
        renderMap();
    }
}

function renderChart() {
    // Aggregate data by country for the chart
    const aggregation = {};
    currentData.forEach(item => {
        const country = item.country_display;
        if (!country) return;

        if (!aggregation[country]) {
            aggregation[country] = { country: country, value: 0 };
        }
        aggregation[country].value += Number(item[currentMetric]) || 0;
    });

    const chartData = Object.values(aggregation).sort((a, b) => b.value - a.value);
    const labels = chartData.map(item => item.country);
    const data = chartData.map(item => item.value);

    const ctx = document.getElementById('dataChart').getContext('2d');

    if (chartInstance) {
        chartInstance.destroy();
    }

    chartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: metricLabels[currentMetric] || currentMetric,
                data: data,
                backgroundColor: currentMetric === 'net_mass_kg' ? 'rgba(52, 168, 83, 0.8)' : 'rgba(26, 115, 232, 0.8)',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: metricLabels[currentMetric] || currentMetric
                    }
                }
            },
            plugins: {
                legend: {
                    display: false
                }
            }
        }
    });
}

    function formatNumber(num, metric = null) {
    if (num === null || num === undefined) return 'N/A';
    const decimals = (metric === 'statistical_value_gbp') ? 2 : 0;
    return num.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

function renderTable(data) {
    const tbody = document.querySelector('#dataTable tbody');
    tbody.innerHTML = '';
    data.forEach(item => {
        const row = document.createElement('tr');
        
        row.innerHTML = `
            <td>${item.country_display || 'N/A'}</td>
            <td>${item.tea_type || 'N/A'}</td>
            <td>${formatNumber(item.net_mass_kg, 'net_mass_kg')}</td>
            <td>${formatNumber(item.statistical_value_gbp, 'statistical_value_gbp')}</td>
        `;
        tbody.appendChild(row);
    });
}

function sortData(data, column, direction) {
        // Ensure data is an array before attempting to sort
    if (!Array.isArray(data)) {
        console.error("Attempted to sort non-array data:", data);
        return [];
    }

    return [...data].sort((a, b) => {
        let valA = a[column];
        let valB = b[column];

        // Handle numeric sorting for specific columns
        if (column === 'net_mass_kg' || column === 'statistical_value_gbp') {
            valA = Number(valA) || 0;
            valB = Number(valB) || 0;
        }

        if (valA < valB) return direction === 'asc' ? -1 : 1;
        if (valA > valB) return direction === 'asc' ? 1 : -1;
        return 0;
    });
}

    function handleSort(event) {
    const column = event.target.dataset.sort;
    if (!column) return;

    if (sortState.column === column) {
        sortState.direction = sortState.direction === 'asc' ? 'desc' : 'asc';
    } else {
        sortState.column = column;
        sortState.direction = 'asc';
    }

    // Remove existing arrows and classes
    document.querySelectorAll('.sort-arrow').forEach(el => el.remove());
    document.querySelectorAll('th').forEach(el => el.classList.remove('sort-asc', 'sort-desc'));

    // Add new arrow and class
    const arrow = document.createElement('span');
    arrow.className = 'sort-arrow';
    arrow.textContent = sortState.direction === 'asc' ? '↑' : '↓';
    event.target.appendChild(arrow);
    event.target.classList.add(sortState.direction === 'asc' ? 'sort-asc' : 'sort-desc');

    // Sort the data
    const sortedData = sortData(currentData, sortState.column, sortState.direction);

    renderTable(sortedData);
}


// --- Map Functions (Leaflet.js) ---
    function initializeMap() {
    if (mapInstance) return;
    if (!document.getElementById('map-container')) return;

    mapInstance = L.map('map-container').setView([20, 0], 2); // Center map globally
        L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
        subdomains: 'abcd',
        maxZoom: 10,
        minZoom: 2
    }).addTo(mapInstance);
}

function renderMap() {
    if (!geojsonData) {
        console.warn("GeoJSON data not available for map rendering.");
        return;
    }
    initializeMap();

    // Clear previous layers
    mapInstance.eachLayer(layer => {
        if (!!layer.toGeoJSON) { // Check if it's a geojson layer
            mapInstance.removeLayer(layer);
        }
    });

    // Aggregate data by country for the map
    const aggregation = {};
    let maxValue = 0;
    currentData.forEach(item => {
        // Standardize country names for mapping
        let country = item.country_display;
        if (!country) return;

        if (country === "Poland (Packing)") country = "Poland";
        if (country === "UAE (Packing)") country = "United Arab Emirates";

        if (!aggregation[country]) {
            aggregation[country] = 0;
        }
        const value = Number(item[currentMetric]) || 0;
        aggregation[country] += value;
        if (aggregation[country] > maxValue) {
            maxValue = aggregation[country];
        }
    });

    // Function to determine color based on value
    const getStyle = (feature) => {
        const countryName = feature.properties.name;
        const value = aggregation[countryName] || 0;
        const intensity = maxValue > 0 ? (value / maxValue) : 0;

        // Use a color scale
        const baseColor = currentMetric === 'net_mass_kg' ? '#34a853' : '#1a73e8';

        return {
            fillColor: baseColor,
            weight: 1,
            opacity: 1,
            color: '#666',
            fillOpacity: intensity * 0.9
        };
    };

    // Function to bind popups
    const onEachFeature = (feature, layer) => {
        const countryName = feature.properties.name;
        const value = aggregation[countryName];
        if (value) {
            layer.bindPopup(`<b>${countryName}</b><br>${metricLabels[currentMetric] || currentMetric}: ${formatNumber(value, currentMetric)}`);
        }
    };

    L.geoJson(geojsonData, {
        style: getStyle,
        onEachFeature: onEachFeature,
        filter: feature => aggregation.hasOwnProperty(feature.properties.name)
    }).addTo(mapInstance);
}


// --- Event Listeners ---
document.addEventListener('DOMContentLoaded', () => {
    // Initialize mobile menu
    if (typeof initializeMobileMenu === 'function') {
        initializeMobileMenu();
    }
    
    const params = new URLSearchParams(window.location.search);
    const datasetFile = params.get('dataset');
    
    if (datasetFile) {
        fetchData(datasetFile);
    } else {
        document.getElementById('dashboard-title').textContent = 'Error: No dataset selected.';
    }

    // Metric slicer logic
    document.querySelectorAll('.slicer-button').forEach(button => {
        button.addEventListener('click', () => {
            document.querySelectorAll('.slicer-button').forEach(btn => btn.classList.remove('active'));
            button.classList.add('active');
            currentMetric = button.dataset.metric;
            updateView();
        });
    });

    // View switcher logic
    document.querySelectorAll('.view-chip').forEach(button => {
        button.addEventListener('click', () => {
            document.querySelectorAll('.view-chip').forEach(btn => btn.classList.remove('active'));
            button.classList.add('active');
            currentView = button.dataset.view;
            updateView();
        });
    });

        // Table sort logic
    document.querySelector('#dataTable thead').addEventListener('click', handleSort);
});