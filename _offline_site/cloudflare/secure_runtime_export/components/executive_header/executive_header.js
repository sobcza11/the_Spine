function executiveHeader(routeLabel, status, timestamp){

    return `

    <section class="executive-header">

        <div>
            <div class="eyebrow">ISOVECTOR / GEOSCEN</div>
            <h1>${routeLabel}</h1>
            <p>Offline institutional recursive cognition console.</p>
        </div>

        <div class="executive-status">
            <span>${status}</span>
            <small>${timestamp}</small>
        </div>

    </section>

    `;
}
