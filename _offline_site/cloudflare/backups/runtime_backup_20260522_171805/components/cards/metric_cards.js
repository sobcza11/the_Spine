function renderMetricCards(metrics){

    return `

    <section class="grid">

        ${(metrics || []).map(m => `

            <article class="metric-card">

                <div class="metric-title">
                    ${safeText(m.label)}
                </div>

                <div class="metric-value">
                    ${safeText(m.value)}
                </div>

                <div class="metric-state">
                    ${safeText(m.state)}
                </div>

            </article>

        `).join("")}

    </section>

    `;
}
