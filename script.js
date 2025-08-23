document.addEventListener('DOMContentLoaded', () => {
    // --- Dynamic Header Scroll Logic ---
    const headerSecondary = document.querySelector('.header-secondary');
    
    if (headerSecondary) {
        let lastScrollTop = 0;
        const scrollThreshold = 10; // Pixels to scroll before triggering
        const headerPrimaryHeight = document.querySelector('.header-primary')?.offsetHeight || 56;

        window.addEventListener('scroll', () => {
            let scrollTop = window.pageYOffset || document.documentElement.scrollTop;

            if (window.innerWidth <= 900) {
                if (Math.abs(scrollTop - lastScrollTop) <= scrollThreshold) {
                    return; 
                }
                if (scrollTop > lastScrollTop && scrollTop > headerPrimaryHeight) {
                    headerSecondary.classList.add('is-hidden');
                } else {
                    headerSecondary.classList.remove('is-hidden');
                }
            }
            
            lastScrollTop = scrollTop <= 0 ? 0 : scrollTop;
        }, false);
    }
});