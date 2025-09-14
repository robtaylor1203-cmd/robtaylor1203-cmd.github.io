// Market Data JavaScript for TeaTrade
// Handles live data visualization with your existing styling

class TeaTradeMarketData {
    constructor() {
        this.charts = {};
        this.data = {
            auctions: null,
            news: null,
            summary: null
        };
        this.init();
    }

    async init() {
        await this.loadAllData();
        this.renderMarketSummary();
        this.renderPriceChart();
        this.renderAuctionTable();
        this.renderVolumeChart();
        this.renderLatestNews();
        this.setupSearch();
        this.startAutoRefresh();
    }

    async loadAllData() {
        const statusElement = document.getElementById('data-status');
        statusElement.textContent = 'Loading market data...';

        try {
            // Load all data endpoints
            const [summaryResponse, auctionResponse, newsResponse] = await Promise.allSettled([
                fetch('data/latest/summary.json'),
                fetch('data/latest/auction_data.json'),
                fetch('data/latest/news_data.json')
            ]);

            // Process responses
            if (summaryResponse.status === 'fulfilled' && summaryResponse.value.ok) {
                this.data.summary = await summaryResponse.value.json();
            }

            if (auctionResponse.status === 'fulfilled' && auctionResponse.value.ok) {
                this.data.auctions = await auctionResponse.value.json();
            }

            if (newsResponse.status === 'fulfilled' && newsResponse.value.ok) {
                this.data.news = await newsResponse.value.json();
            }

            // Check if any data loaded
            if (this.data.summary || this.data.auctions || this.data.news) {
                statusElement.textContent = `Live data updated ${this.formatTime(new Date())}`;
            } else {
                throw new Error('No data sources available');
            }

        } catch (error) {
            console.warn('Live data not available, loading demo data:', error.message);
            this.loadDemoData();
            statusElement.textContent = 'Demo mode - Live data will be available after automation setup';
        }
    }

    loadDemoData() {
        // Generate realistic demo data
        this.data.summary = {
            last_updated: new Date().toISOString(),
            total_auctions: 156,
            total_news: 18,
            locations: ['Kolkata', 'Guwahati', 'Colombo', 'Mombasa'],
            currencies: ['INR', 'LKR', 'KES']
        };

        // Demo auction data
        this.data.auctions = this.generateDemoAuctions();
        
        // Demo news data
        this.data.news = [
            {
                title: "Tea Auction Prices Rise Significantly in Major Centers",
                source: "Tea Trade Weekly",
                url: "#",
                summary: "Recent market analysis shows price increases across multiple auction centers.",
                publish_date: new Date(Date.now() - 2*60*60*1000).toISOString()
            },
            {
                title: "Ceylon Tea Production Shows Strong Recovery",
                source: "Market Intelligence",
                url: "#",
                summary: "Sri Lankan tea production bounces back after seasonal challenges.",
                publish_date: new Date(Date.now() - 6*60*60*1000).toISOString()
            },
            {
                title: "Kenya Auction Reports Record Volume",
                source: "East Africa Tea Report",
                url: "#",
                summary: "Mombasa auction sees exceptional trading volumes this week.",
                publish_date: new Date(Date.now() - 12*60*60*1000).toISOString()
            }
        ];
    }

    generateDemoAuctions() {
        const locations = ['Kolkata', 'Guwahati', 'Colombo', 'Mombasa'];
        const grades = ['BOP', 'PEKOE', 'OP', 'BOPF', 'PEK1', 'FBOP'];
        const gardens = ['Garden Estate A', 'Tea Co B', 'Plantation C', 'Estate D', 'Gardens E'];
        
        const auctions = [];
        
        for (let i = 0; i < 50; i++) {
            const daysBack = Math.floor(Math.random() * 7);
            const date = new Date(Date.now() - daysBack * 24 * 60 * 60 * 1000);
            
            auctions.push({
                location: locations[Math.floor(Math.random() * locations.length)],
                grade: grades[Math.floor(Math.random() * grades.length)],
                garden_name: gardens[Math.floor(Math.random() * gardens.length)],
                price: Math.round((100 + Math.random() * 300) * 100) / 100,
                quantity: Math.floor(50 + Math.random() * 450),
                currency: 'USD',
                auction_date: date.toISOString()
            });
        }
        
        return auctions.sort((a, b) => new Date(b.auction_date) - new Date(a.auction_date));
    }

    renderMarketSummary() {
        if (!this.data.auctions || this.data.auctions.length === 0) return;

        const activeAuctions = this.data.auctions.length;
        const avgPrice = this.data.auctions.reduce((sum, auction) => sum + auction.price, 0) / activeAuctions;
        const totalVolume = this.data.auctions.reduce((sum, auction) => sum + auction.quantity, 0);

        document.getElementById('active-auctions').textContent = activeAuctions;
        document.getElementById('avg-price').textContent = `$${avgPrice.toFixed(2)}`;
        document.getElementById('total-volume').textContent = `${totalVolume.toLocaleString()}`;
    }

    renderPriceChart() {
        if (!this.data.auctions) return;

        const ctx = document.getElementById('priceChart').getContext('2d');
        
        // Group data by date
        const dailyPrices = {};
        this.data.auctions.forEach(auction => {
            const date = new Date(auction.auction_date).toLocaleDateString();
            if (!dailyPrices[date]) {
                dailyPrices[date] = [];
            }
            dailyPrices[date].push(auction.price);
        });

        // Calculate daily averages
        const chartData = Object.keys(dailyPrices)
            .sort((a, b) => new Date(a) - new Date(b))
            .slice(-7) // Last 7 days
            .map(date => ({
                date: date,
                avgPrice: dailyPrices[date].reduce((sum, price) => sum + price, 0) / dailyPrices[date].length
            }));

        this.charts.priceChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: chartData.map(item => item.date),
                datasets: [{
                    label: 'Average Price (USD)',
                    data: chartData.map(item => item.avgPrice),
                    borderColor: '#1a73e8',
                    backgroundColor: 'rgba(26, 115, 232, 0.1)',
                    borderWidth: 2,
                    fill: true,
                    tension: 0.4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        display: false
                    }
                },
                scales: {
                    y: {
                        beginAtZero: false,
                        ticks: {
                            callback: function(value) {
                                return '$' + value.toFixed(2);
                            }
                        }
                    }
                }
            }
        });
    }

    renderAuctionTable() {
        if (!this.data.auctions) return;

        const tbody = document.getElementById('auction-data-body');
        
        // Show recent 20 auctions
        const recentAuctions = this.data.auctions.slice(0, 20);
        
        tbody.innerHTML = recentAuctions.map(auction => `
            <tr>
                <td>${new Date(auction.auction_date).toLocaleDateString()}</td>
                <td>${auction.location}</td>
                <td><strong>${auction.grade}</strong></td>
                <td>${auction.garden_name}</td>
                <td>$${auction.price.toFixed(2)}</td>
                <td>${auction.quantity.toLocaleString()} kg</td>
            </tr>
        `).join('');
    }

    renderVolumeChart() {
        if (!this.data.auctions) return;

        const ctx = document.getElementById('volumeChart').getContext('2d');
        
        // Group by location
        const locationVolumes = {};
        this.data.auctions.forEach(auction => {
            if (!locationVolumes[auction.location]) {
                locationVolumes[auction.location] = 0;
            }
            locationVolumes[auction.location] += auction.quantity;
        });

        const colors = ['#1a73e8', '#34a853', '#fbbc04', '#ea4335', '#9aa0a6'];
        
        this.charts.volumeChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: Object.keys(locationVolumes),
                datasets: [{
                    data: Object.values(locationVolumes),
                    backgroundColor: colors.slice(0, Object.keys(locationVolumes).length),
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom'
                    }
                }
            }
        });
    }

    renderLatestNews() {
        if (!this.data.news || this.data.news.length === 0) return;

        const newsList = document.getElementById('market-news');
        const latestNews = this.data.news.slice(0, 5); // Show 5 latest

        newsList.innerHTML = latestNews.map(article => `
            <li class="news-item">
                <div class="text-content">
                    <div class="source">${article.source} • ${this.formatTime(new Date(article.publish_date))}</div>
                    <div class="main-link">
                        <a href="${article.url}" target="_blank">
                            <h3>${article.title}</h3>
                        </a>
                        <div class="snippet">${article.summary}</div>
                    </div>
                </div>
            </li>
        `).join('');
    }

    setupSearch() {
        const searchBar = document.querySelector('.header-search-bar');
        if (searchBar) {
            searchBar.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.filterData(e.target.value);
                }
            });
        }
    }

    filterData(query) {
        if (!query.trim() || !this.data.auctions) return;
        
        const filtered = this.data.auctions.filter(auction =>
            auction.location.toLowerCase().includes(query.toLowerCase()) ||
            auction.grade.toLowerCase().includes(query.toLowerCase()) ||
            auction.garden_name.toLowerCase().includes(query.toLowerCase())
        );

        // Update table with filtered results
        const tbody = document.getElementById('auction-data-body');
        tbody.innerHTML = filtered.slice(0, 20).map(auction => `
            <tr style="background-color: rgba(26, 115, 232, 0.05);">
                <td>${new Date(auction.auction_date).toLocaleDateString()}</td>
                <td>${auction.location}</td>
                <td><strong>${auction.grade}</strong></td>
                <td>${auction.garden_name}</td>
                <td>$${auction.price.toFixed(2)}</td>
                <td>${auction.quantity.toLocaleString()} kg</td>
            </tr>
        `).join('');
    }

    formatTime(date) {
        const now = new Date();
        const diffMs = now - date;
        const diffHours = Math.floor(diffMs / (1000 * 60 * 60));
        
        if (diffHours < 1) return 'Just now';
        if (diffHours < 24) return `${diffHours}h ago`;
        return date.toLocaleDateString();
    }

    startAutoRefresh() {
        // Refresh every 10 minutes
        setInterval(async () => {
            await this.loadAllData();
            this.renderMarketSummary();
            this.renderAuctionTable();
            this.renderLatestNews();
        }, 10 * 60 * 1000);
    }
}

// Initialize when DOM loads
document.addEventListener('DOMContentLoaded', () => {
    new TeaTradeMarketData();
});

// Export for potential use by other modules
window.TeaTradeMarketData = TeaTradeMarketData;
