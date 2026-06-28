# Sistema Distribuido: Detección de Patrones de Lavado de Dinero

## Tareas recurrentes

- Para generar el compose, levantar y ejecutar el sistema: `make up`.
- Para bajar y eliminar contenedores: `make down`.
- Para ver logs: `make logs`. Se puede agregar `grep` para filtrar la vista, por ejemplo: `make logs | grep -v rabbit`.
- Para ver los logs internos de los `fault_hypervisor`, entrar al contenedor correspondiente y ejecutar `docker logs <nombre_de_contenedor>`.
- Para eliminar workers, usar `make real_chaos`. Se pueden agregar parámetros especiales para matar nodos específicos en un intervalo específico. Por ejemplo, `make real_chaos INTERVAL=10 TARGET=stateful` eliminará cualquier nodo stateful cada 10 segundos. El `TARGET` puede ser cualquier nombre de worker desplegado, o bien agrupar usando `stateful` o `stateless`.
- Para ejecutar la caída de todos los nodos trabajadores: `make catastrophe`.
- Para dar de baja un `fault_hypervisor`, ejecutar `docker kill fault_hypervisor_X`, reemplazando `X` por el número de hipervisor a eliminar.

*Nota: para la finalización efectiva del procesamiento del dataset mediano, se recomienda no usar un `INTERVAL` menor a 7 segundos.*

## Verificación de outputs

- Para comparar la salida con el resultado esperado: `make medium_test CLIENT=X` o `make small_test CLIENT=X`, reemplazando `X` por el número de cliente a verificar. Si no se indica `CLIENT`, se usa `CLIENT=1`.
- Los datasets deben estar en una carpeta `datasets` en la raíz del proyecto, con el formato `client_X_transactions.csv` y `client_X_accounts.csv`.
- Los resultados esperados deben estar en `expected_outputs/expected_hi_medium` o `expected_outputs/expected_hi_small`, con archivos `q1_results.csv`, `q2_results.csv`, ..., `q5_results.csv`.
- Los outputs generados por el sistema deben estar en `outputs/client_X`, con archivos `q1_result.csv`, `q2_result.csv`, ..., `q5_result.csv`.

## Notebooks de referencia para comparar salidas

- Patrones base: https://colab.research.google.com/drive/1bLKHk3lqPw0-6gaPu8phhWIU0gX7PHnt?usp=sharing
- Patrón Scatter Gather: https://colab.research.google.com/drive/1n6oZP-nV-vZHZhaE6HYbxxh_r-oMO7Jj?usp=sharing
