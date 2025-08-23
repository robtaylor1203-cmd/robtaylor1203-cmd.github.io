document.addEventListener('DOMContentLoaded', () => {
    // --- Dynamic Header Scroll Logic ---
    const headerSecondary = document.querySelector('.header-secondary');
    if (headerSecondary) {
        let lastScrollTop = 0;
        const scrollThreshold = 5; // Pixels to scroll before triggering

        window.addEventListener('scroll', () => {
            let scrollTop = window.pageYOffset || document.documentElement.scrollTop;

            if (Math.abs(scrollTop - lastScrollTop) <= scrollThreshold) {
                return; // Do nothing if scroll is too small
            }

            if (scrollTop > lastScrollTop && scrollTop > headerSecondary.offsetHeight) {
                // Scrolling Down
                headerSecondary.classList.add('is-hidden');
            } else {
                // Scrolling Up
                headerSecondary.classList.remove('is-hidden');
            }
            lastScrollTop = scrollTop <= 0 ? 0 : scrollTop; // For Mobile or negative scrolling
        }, false);
    }
});