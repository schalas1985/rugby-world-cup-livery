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

const predictorFixtures = document.querySelectorAll(".predictor-fixture");

predictorFixtures.forEach((fixture) => {
  const winnerInputs = fixture.querySelectorAll(".predictor-winner-toggle input");
  const pointSpans = fixture.querySelectorAll(".predictor-log-points span");

  const syncPoints = () => {
    pointSpans.forEach((span) => {
      span.textContent = "0 log points";
    });

    winnerInputs.forEach((input, index) => {
      if (input.checked && pointSpans[index]) {
        pointSpans[index].textContent = "4 log points";
      }
    });
  };

  winnerInputs.forEach((input) => {
    input.addEventListener("change", syncPoints);
  });

  syncPoints();
});
