-- Activos por curso
SELECT 
  c.titulo AS Curso,
  COUNTIF(e.estado = 'Activo') AS Estudiantes
  
FROM `registros8.enrollments` e
JOIN `registros8.courses` c ON e.course_id = c.id
GROUP BY c.titulo;
-- Promedio

SELECT 
  c.titulo AS Curso,
  COUNTIF(e.estado = 'Activo') AS Activos,
  ROUND(AVG(e.puntaje), 2) AS PromedioCurso
FROM `registros8.enrollments` e
JOIN `registros8.courses` c ON e.course_id = c.id
GROUP BY c.titulo;

select * from `registros8.enrollments`