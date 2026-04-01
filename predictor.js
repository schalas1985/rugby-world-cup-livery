const predictorTabs = document.querySelectorAll(".predictor-tabs__button");
const predictorPanels = document.querySelectorAll(".predictor-panel");

predictorTabs.forEach((button) => {
  button.addEventListener("click", () => {
    const target = button.dataset.target;

    predictorTabs.forEach((tab) => {
      const active = tab === button;
      tab.classList.toggle("is-active", active);
      tab.setAttribute("aria-selected", active ? "true" : "false");
    });

    predictorPanels.forEach((panel) => {
      const active = panel.id === target;
      panel.classList.toggle("is-active", active);
      panel.hidden = !active;
    });
  });
});
