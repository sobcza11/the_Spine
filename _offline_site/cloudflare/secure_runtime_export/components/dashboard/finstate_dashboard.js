function renderFinStateDashboard(payloads){

    return `

        ${renderSurvivabilityCommandLayer(payloads)}

        <div class="dashboard-layout">

            <div class="executive-panel">
                ${renderExecutiveCards(payloads)}
            </div>

            <div class="executive-panel">
                ${renderSurvivabilityLayer(payloads)}
            </div>

            <div class="executive-panel">
                ${renderCorporateSystemicity(payloads)}
            </div>

            <div class="executive-panel">
                ${renderRecursiveDeteriorationMonitoring(payloads)}
            </div>

            <div class="executive-panel">
                ${renderRecursiveFragility(payloads)}
            </div>

            <div class="executive-panel">
                ${renderPersistenceEscalation(payloads)}
            </div>

            <div class="executive-panel">
                ${renderAntiFragilityLayer(payloads)}
            </div>

            <div class="executive-panel">
                ${renderSurvivabilityDrift(payloads)}
            </div>

            <div class="executive-panel">
                ${renderInstitutionalDeteriorationHeatmap(payloads)}
            </div>

            <div class="executive-panel">
                ${renderSectorDeteriorationSync(payloads)}
            </div>

            <div class="executive-panel">
                ${renderBalanceSheetTopology(payloads)}
            </div>

            <div class="executive-panel">
                ${renderGeoScenFinStateHooks(payloads)}
            </div>

        </div>

        ${renderRuntimeNarratives(payloads)}

        ${renderInstitutionalTables(payloads)}

    `;
}
