-- DROP FUNCTION public.run_cron_tasks();
CREATE OR REPLACE FUNCTION public.run_cron_tasks()
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    task RECORD;
    tasks_executed INTEGER := 0;
BEGIN
    -- Проходим по всем активным задачам
    FOR task IN 
        SELECT * FROM public.scheduled_tasks
        WHERE 
            is_active
            AND (last_run IS NULL OR last_run + schedule <= NOW())
        ORDER BY created_at
    LOOP
        BEGIN
            -- Выводим информацию о выполняемой задаче
            RAISE NOTICE 'Выполняю задачу: %', task.task_name;            
            -- Выполняем запрос
            EXECUTE task.query;            
            -- Обновляем время последнего выполнения
            UPDATE scheduled_tasks
            SET last_run = NOW(),
                last_error = NULL -- Сбрасываем ошибку, если она была
            WHERE id = task.id;            
            -- Увеличиваем счетчик выполненных задач
            tasks_executed := tasks_executed + 1;
        EXCEPTION WHEN OTHERS THEN
            -- Ловим ошибку и сохраняем её в базу
            UPDATE scheduled_tasks
            SET last_error = SQLERRM
            WHERE id = task.id;
        END;
    END LOOP;
    -- Возвращаем количество выполненных задач
    RETURN tasks_executed;
END;
$function$
;
