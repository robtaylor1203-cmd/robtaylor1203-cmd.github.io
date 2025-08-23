document.addEventListener('DOMContentLoaded', () => {
    // --- Dynamic Header Scroll Logic ---
    const headerSecondary = document.querySelector('.header-secondary');
    
    if (headerSecondary) {
        let lastScrollTop = 0;
        const scrollThreshold = 5; // How many pixels to scroll before we trigger the hide/show

        window.addEventListener('scroll', () => {
            let scrollTop = window.pageYOffset || document.documentElement.scrollTop;

            // Only apply this dynamic behavior on mobile screens
            if (window.innerWidth <= 900) {
                if (Math.abs(scrollTop - lastScrollTop) <= scrollThreshold) {
                    return; // Do nothing if it's just a tiny scroll
                }

                if (scrollTop > lastScrollTop && scrollTop > 100) {
                    // If scrolling Down and past 100px from the top, hide the bar
                    headerSecondary.classList.add('is-hidden');
                } else {
                    // If scrolling Up, show the bar
                    headerSecondary.classList.remove('is-hidden');
                }
            }
            
            lastScrollTop = scrollTop <= 0 ? 0 : scrollTop;
        }, false);
    }
});