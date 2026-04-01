const selectorLinks = document.querySelectorAll(".tournament-hero__selector a");
const scrollKey = "rwc-tournament-scroll-y";

selectorLinks.forEach((link) => {
  link.addEventListener("click", () => {
    sessionStorage.setItem(scrollKey, String(window.scrollY));
  });
});

const savedScroll = sessionStorage.getItem(scrollKey);

if (savedScroll !== null) {
  const targetY = Number(savedScroll);
  window.addEventListener("load", () => {
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        window.scrollTo(0, targetY);
        sessionStorage.removeItem(scrollKey);
      });
    });
  });
}
