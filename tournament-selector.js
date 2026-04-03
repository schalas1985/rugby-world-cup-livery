const scrollKey = "rwc-tournament-scroll-y";
const savedScroll = sessionStorage.getItem(scrollKey);

if (savedScroll !== null) {
  document.documentElement.classList.add("selector-restoring");
}

window.addEventListener("DOMContentLoaded", () => {
  const selectorLinks = document.querySelectorAll(".tournament-hero__selector a");

  selectorLinks.forEach((link) => {
    link.addEventListener("click", () => {
      sessionStorage.setItem(scrollKey, String(window.scrollY));
    });
  });
});

if (savedScroll !== null) {
  const targetY = Number(savedScroll);
  window.addEventListener("load", () => {
    window.scrollTo(0, targetY);
    requestAnimationFrame(() => {
      document.documentElement.classList.remove("selector-restoring");
      sessionStorage.removeItem(scrollKey);
    });
  });
}
