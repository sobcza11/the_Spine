function renderTable(title, headers, rows){

    return `

    <article class="narrative-block">

        <h2>${title}</h2>

        <table>

            <thead>
                <tr>
                    ${headers.map(h => `<th>${h}</th>`).join("")}
                </tr>
            </thead>

            <tbody>

                ${rows.map(row => `
                    <tr>
                        ${row.map(cell => `<td>${safeText(cell)}</td>`).join("")}
                    </tr>
                `).join("")}

            </tbody>

        </table>

    </article>

    `;
}
