/**
 * Global JavaScript for TeaTrade Platform
 * 
 * - Mobile Navigation Menu Toggle
 */

// Function to initialize the mobile menu toggle functionality
function initializeMobileMenu() {
    const toggleButton = document.querySelector('.mobile-menu-toggle');
    const navigationMenu = document.querySelector('.secondary-nav');

    if (toggleButton && navigationMenu) {
        toggleButton.addEventListener('click', () => {
            const isExpanded = toggleButton.getAttribute('aria-expanded') === 'true';
            
            // Toggle the 'is-active' class for CSS styling
            navigationMenu.classList.toggle('is-active');
            
            // Update ARIA attributes for accessibility
            toggleButton.setAttribute('aria-expanded', !isExpanded);
        });
    }
}

// Note: The initialization of this script (calling initializeMobileMenu) 
// is handled within the specific page's <script> block 
// during the DOMContentLoaded event (e.g., in report-viewer.html or market-reports.html).