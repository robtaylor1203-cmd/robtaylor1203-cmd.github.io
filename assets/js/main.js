// Simple TeaTrade enhancements - preserves your beautiful design
class TeaTradeHome {
    constructor() {
        this.init();
    }

    async init() {
        // Only show status if data is available
        await this.checkForData();
        this.setupSearch();
    }

    async checkForData() {
        try {
            const response = await fetch('data/latest/summary.json');
            if (response.ok) {
                const data = await response.json();
                this.showStatus(data);
            }
        } catch (error) {
            // Silently fail - no status shown if no data
            console.log('Running in basic mode');
        }
    }

    showStatus(data) {
        const statusElement = document.getElementById('system-status');
        if (statusElement && data) {
            const updateTime = new Date(data.last_updated).toLocaleDateString();
            statusElement.querySelector('.status-text').textContent = `Live Data • Updated ${updateTime}`;
            statusElement.style.display = 'flex';
        }
    }

    setupSearch() {
        const searchBar = document.querySelector('.search-bar');
        if (searchBar) {
            searchBar.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    const query = e.target.value.trim();
                    if (query) {
                        // Simple search routing
                        if (query.toLowerCase().includes('market')) {
                            window.location.href = 'market-data.html';
                        } else if (query.toLowerCase().includes('news')) {
                            window.location.href = 'news.html';
                        } else if (query.toLowerCase().includes('job')) {
                            window.location.href = 'jobs.html';
                        } else {
                            window.location.href = `news.html?search=${encodeURIComponent(query)}`;
                        }
                    }
                }
            });
        }
    }
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', () => {
    new TeaTradeHome();
});
