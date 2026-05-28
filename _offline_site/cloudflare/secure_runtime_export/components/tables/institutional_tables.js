function renderInstitutionalTables(payloads){

    const sections = [];

    (payloads || []).forEach(x => {

        const p = x.payload?.payload || x.payload || {};

        if(Array.isArray(p.message_bus)){

            const rows = p.message_bus.map(m => [
                m.sender,
                m.target,
                m.message_type
            ]);

            sections.push(
                renderTable(
                    "Message Bus",
                    ["Sender", "Target", "Type"],
                    rows
                )
            );
        }

        if(Array.isArray(p.cognition_rows)){

            const rows = p.cognition_rows.map(r => [
                r.bank_code || "--",
                r.zone || "--",
                r.cognition_state || "--",
                r.policy_classification || "--"
            ]);

            sections.push(
                renderTable(
                    "CB Cognition",
                    ["Bank", "Zone", "State", "Policy"],
                    rows
                )
            );
        }

        if(Array.isArray(p.signal_rows)){

            const rows = p.signal_rows.map(r => [
                r.signal_family || "--",
                r.macro_state || "--",
                r.source || "--",
                r.available ?? "--"
            ]);

            sections.push(
                renderTable(
                    "Structural Signals",
                    ["Family", "State", "Source", "Available"],
                    rows
                )
            );
        }
    });

    return sections.join("");
}
