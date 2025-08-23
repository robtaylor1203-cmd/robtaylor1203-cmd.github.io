document.addEventListener('DOMContentLoaded', () => {
    // --- Dynamic Header Scroll Logic ---
    const headerSecondary = document.querySelector('.header-secondary');
    
    // Check if the element exists (it won't on the simple landing pages)
    if (headerSecondary) {
        let lastScrollTop = 0;
        const scrollThreshold = 10; // Pixels to scroll before triggering
        const headerPrimaryHeight = document.querySelector('.header-primary')?.offsetHeight || 56;

        window.addEventListener('scroll', () => {
            let scrollTop = window.pageYOffset || document.documentElement.scrollTop;

            // Only apply the effect on mobile view
            if (window.innerWidth <= 900) {
                if (Math.abs(scrollTop - lastScrollTop) <= scrollThreshold) {
                    return; // Do nothing if scroll is too small
                }

                if (scrollTop > lastScrollTop && scrollTop > headerPrimaryHeight) {
                    // Scrolling Down
                    headerSecondary.classList.add('is-hidden');
                } else {
                    // Scrolling Up
                    headerSecondary.classList.remove('is-hidden');
                }
            }
            
            lastScrollTop = scrollTop <= 0 ? 0 : scrollTop; // For Mobile or negative scrolling
        }, false);
    }
});