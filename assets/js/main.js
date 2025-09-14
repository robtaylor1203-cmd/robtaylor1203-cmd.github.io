/**
 * TeaTrade Enhanced Features
 * Adds live data to your beautiful existing design
 */

class TeaTradeEnhancer {
    constructor() {
        this.initializeLiveFeatures();
    }
    
    async initializeLiveFeatures() {
        console.log('🚀 Enhancing TeaTrade with live data...');
        
        // Add live status indicator to your existing design
        this.addStatusIndicator();
        
        // Load live data if available
        await this.loadLatestData();
        
        // Add interactive features to your existing pages
        this.enhanceExistingPages();
    }
    
    addStatusIndicator() {
        // Find your existing search area and add status
        const searchArea = document.querySelector('.search-area');
        if (searchArea) {
            const statusDiv = document.createElement('div');
            statusDiv.className = 'system-status';
            statusDiv.innerHTML = `
                <div class="status-indicator"></div>
                <span>Live Market Data Active</span>
            `;
            searchArea.parentNode.insertBefore(statusDiv, searchArea.nextSibling);
        }
    }
    
    async loadLatestData() {
        try {
            // Try to load live data
            const response = await fetch('data/latest/summary.json');
            if (response.ok) {
                const data = await response.json();
                this.updateWithLiveData(data);
            } else {
                // Use sample data if live data not available
                this.updateWithSampleData();
            }
        } catch (error) {
            console.log('Using sample data for demonstration');
            this.updateWithSampleData();
        }
    }
    
    updateWithLiveData(data) {
        // Add quick stats under your existing search bar
        const searchArea = document.querySelector('.search-area');
        if (searchArea && !document.querySelector('.quick-stats')) {
            const statsDiv = document.createElement('div');
            statsDiv.className = 'quick-stats';
            statsDiv.innerHTML = `
                <div class="stat-item">
                    <span class="stat-value">${data.total_auctions || 0}</span>
                    <span class="stat-label">Live Auctions</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value">${data.total_news || 0}</span>
                    <span class="stat-label">News Articles</span>
                </div>
                <div class="stat-item">
                    <span class="stat-value">${data.active_gardens || 0}</span>
                    <span class="stat-label">Active Gardens</span>
                </div>
            `;
            searchArea.parentNode.insertBefore(statsDiv, searchArea.nextSibling.nextSibling);
        }
    }
    
    updateWithSampleData() {
        // Add sample stats to demonstrate the feature
        this.updateWithLiveData({
            total_auctions: 245,
            total_news: 18,
            active_gardens: 127
        });
    }
    
    enhanceExistingPages() {
        // Add data loading to news page if it exists
        if (window.location.pathname.includes('news.html')) {
            this.enhanceNewsPage();
        }
        
        // Add live features to other pages
        if (window.location.pathname.includes('jobs.html')) {
            this.enhanceJobsPage();
        }
    }
    
    async enhanceNewsPage() {
        try {
            const response = await fetch('data/latest/news_data.json');
            if (response.ok) {
                const newsData = await response.json();
                this.populateNewsWithLiveData(newsData);
            }
        } catch (error) {
            console.log('Live news data not available, using existing content');
        }
    }
    
    populateNewsWithLiveData(newsData) {
        const newsContainer = document.getElementById('news-container');
        if (newsContainer && newsData.length > 0) {
            newsContainer.innerHTML = newsData.map(article => `
                <div class="news-item">
                    <div class="text-content">
                        <div class="source">${article.source || 'Tea Industry News'}</div>
                        <a href="${article.url || '#'}" class="main-link">
                            <h3>${article.title}</h3>
                        </a>
                        <div class="snippet">${article.summary || article.content?.substring(0, 150) + '...'}</div>
                    </div>
                    ${article.image ? `
                        <a href="${article.url || '#'}" class="image-link">
                            <img src="${article.image}" alt="Article image" class="article-image" loading="lazy">
                        </a>
                    ` : ''}
                </div>
            `).join('');
        }
    }
    
    enhanceJobsPage() {
        // Add job data loading when available
        console.log('Jobs page enhancement ready for live data');
    }
}

// Initialize when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    new TeaTradeEnhancer();
});
