use {glommio::GlommioError, std::io};

pub fn glommio_io_error<E>(error: E) -> GlommioError<()>
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    GlommioError::IoError(io::Error::new(io::ErrorKind::Other, error))
}
