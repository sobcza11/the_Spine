function runtimeGuard(label, fn){

    try{

        return fn();

    }catch(err){

        console.error(`Runtime guard caught error in ${label}:`, err);

        return `
            <div class="narrative-block">
                <h2>${label}</h2>
                <p>Runtime rendering failed safely. Payload requires review.</p>
            </div>
        `;
    }
}

function safeText(value, fallback="Unavailable"){

    if(value === null || value === undefined || value === ""){
        return fallback;
    }

    return value;
}
