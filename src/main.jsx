import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import App from './App.jsx'

createRoot(document.getElementById('root')).render(
  <StrictMode>
    <App />
  </StrictMode>
)
```

---

**Archivo 5: renombrar `librodiario.jsx` → `src/App.jsx`**

GitHub no permite mover archivos directamente. Haz esto:
1. Abre `librodiario.jsx` → click lápiz ✏️
2. En el campo del nombre arriba, borra `librodiario.jsx` y escribe `src/App.jsx`
3. Scroll abajo → **Commit changes**

Eso mueve el archivo a la carpeta `src` y lo renombra a `App.jsx`.

---

Una vez que hagas los 5 pasos, tu repo debería verse así:
```
index.html
package.json
vite.config.js
vercel.json
src/
    App.jsx
    main.jsx
