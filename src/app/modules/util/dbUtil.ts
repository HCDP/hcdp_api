export function parseParams(paramListArr: string[], allParams: string[], whereClauses: string[], column: string) {
  let orClauses: string[] = [];
  let paramSet = new Set(paramListArr);
  if(paramSet.has(null)) {
    orClauses.push(`${column} IS NULL`);
    paramSet.delete(null);
  }
  
  if(paramSet.size == 1) {
    let param = Array.from(paramSet)[0];
    allParams.push(param);
    orClauses.push(`${column} = $${allParams.length}`);
  }
  else if(paramSet.size > 1) {
    let paramIndices: string[] = [];
    for(let param of paramSet.values()) {
      allParams.push(param);
      paramIndices.push(`$${allParams.length}`);
    }
    orClauses.push(`${column} IN (${paramIndices.join(",")})`);
  }
  
  if(orClauses.length == 1) {
    whereClauses.push(orClauses[0]);
  }
  else if(orClauses.length > 1) {
    whereClauses.push(`(${orClauses.join(" OR ")})`);
  }
}