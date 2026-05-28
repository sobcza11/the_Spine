function renderSurvivabilityCommandLayer(payloads){

    const synthesis = (payloads || []).find(
        x => JSON.stringify(x.payload || {}).includes("finstate_executive_synthesis")
    );

    const p = synthesis?.payload?.payload || synthesis?.payload || {};

    return `

        <section class="narrative-block">

            <h2>Institutional Survivability Command Layer</h2>

            <p>${safeText(p.interpretation, "FINSTATE survivability command layer initialized.")}</p>

            <div class="badge">
                ${safeText(p.executive_state, "finstate_runtime_active")}
            </div>

        </section>

    `;
}
