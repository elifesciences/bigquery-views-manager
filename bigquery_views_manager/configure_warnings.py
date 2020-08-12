import warnings


warnings.filterwarnings(
    'ignore', 'Your application has authenticated using end user credentials',
    category=UserWarning
)
