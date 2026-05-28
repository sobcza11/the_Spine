function metricCard(title, value, state){

    return `

    <div class="metric-card">

        <div class="metric-title">
            ${title}
        </div>

        <div class="metric-value">
            ${value}
        </div>

        <div class="metric-state">
            ${state}
        </div>

    </div>

    `;
}
