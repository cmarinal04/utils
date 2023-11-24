import pandas as pd
from utils.send_detailed_mail import read_ProcesamientoEspecializado_Inicializar, update_GeneracionEspecializado_Obtener, update_status_file
from utils.send_detailed_mail import read_GeneracionEspecializado_Obtener, read_firs_sql_data, update_creation_file, update_ended_file,create_file
from utils.send_email import send_email_smtp


def main():
    PaqueteSISSiD = 25

    ProcesamientoEspecializadoId = read_ProcesamientoEspecializado_Inicializar(PaqueteSISSiD)
    df = read_GeneracionEspecializado_Obtener(ProcesamientoEspecializadoId)
    
    #Variables del procesamiento

    GeneracionEspecializadoId = df.iloc[0, 0]
    GeneracionEspecializadoGuid = df.iloc[0, 1]
    GeneracionEspecializadoParametros = df.iloc[0, 2]
    sql_data = df.iloc[0, 3]
    GeneracionEspecializadoCorreoElectronico = df.iloc[0, 4]
    NotificacionAsunto = df.iloc[0, 5]
    OpcionesGeneracion = df.iloc[0, 6]
    OpcionesGeneracionDetalle = df.iloc[0, 7]
    TipoDigitacionId = df.iloc[0, 8]
    GeneracionEspecializadoNotificarOpciones = df.iloc[0, 9]
    GeneracionEspecializadoFechaCaducidadArchivo = df.iloc[0, 10]
    ComunidadId = df.iloc[0, 11]
    GeneracionEspecializadoCampos = df.iloc[0, 12]
    GeneracionEspecializadoSqlWhere = df.iloc[0, 13]
    OpcionGeneracionEspecializadoProcedimientoAlmacenadoAjustePrevioDigitacion = df.iloc[0, 14]
    OpcionGeneracionEspecializadoProcedimientoAlmacenadoAjustePosteriorDigitacion = df.iloc[0, 15]
    OrigenId = df.iloc[0, 16]
    TipoDigitacionProcedimientoAlmacenadoDefinicionExclusion = df.iloc[0, 17]
    InformeId = df.iloc[0, 18]
    GeneracionEspecializadoConSuscripcion = df.iloc[0, 19]
    SuscripcionPerioricidadNombre = df.iloc[0, 20]
    update_GeneracionEspecializado_Obtener(GeneracionEspecializadoId)
    
    resultado_df = read_firs_sql_data(sql_data)
    update_status_file(GeneracionEspecializadoId)
    update_creation_file(GeneracionEspecializadoId)
    create_file(resultado_df)
    send_email_smtp(email_user, subject_email, body, name_file)
    update_ended_file(GeneracionEspecializadoId)



if __name__ == '__main__':
    main()

