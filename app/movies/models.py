import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True, null=True)

    class Meta:
        db_table = 'content"."genre'
        verbose_name = _('Genre')
        verbose_name_plural = _('Genres')

    def __str__(self):
        return self.name


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    genre = models.ForeignKey('Genre', on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content"."genre_film_work'
        constraints = [
            models.UniqueConstraint(fields=['genre_id', 'film_work_id'], name='unique_genre'),
        ]


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full_name'), unique=True, max_length=255)

    class Meta:
        db_table = 'content"."person'
        verbose_name = _('Person')
        verbose_name_plural = _('Persons')

    def __str__(self):
        return self.full_name


class PersonFilmwork(UUIDMixin):
    film_work = models.ForeignKey('Filmwork', on_delete=models.CASCADE)
    person = models.ForeignKey('Person', on_delete=models.CASCADE)
    class PersonRole(models.TextChoices):
        ACTOR = 'actor'
        DIRECTOR = 'director'
        WRITER = 'writor'

    role = models.CharField(_('role'), choices=PersonRole.choices, default=PersonRole.ACTOR, max_length=8)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content"."person_film_work'
        indexes = [
            models.Index(fields=['person_id', 'film_work_id']),
        ]


class Filmwork(UUIDMixin, TimeStampedMixin):
    title = models.CharField(_('title'), max_length=255)
    description = models.TextField(_('description'), blank=True, null=True)
    creation_date = models.DateTimeField(_('creation_date'), blank=True, null=True)
    certificate = models.CharField(_('certificate'), max_length=512, blank=True)
    file_path = models.FileField(_('file'), blank=True, null=True, upload_to='movies/')
    rating = models.FloatField(
        _('rating'), validators=[MinValueValidator(0), MaxValueValidator(100)], blank=True, null=True
    )
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    persons = models.ManyToManyField(Person, through='PersonFilmwork')

    class FilmworkType(models.TextChoices):
        MOVIE = 'movie'
        TV_SHOW = 'tv_show'

    type = models.CharField('Тип', choices=FilmworkType.choices, default=FilmworkType.MOVIE, max_length=7)

    class Meta:
        db_table = 'content"."film_work'
        verbose_name = _('Film_work')
        verbose_name_plural = _('Film_works')
        indexes = [
            models.Index(fields=['creation_date']),
        ]

    def __str__(self):
        return f'{self.title} {self.creation_date} {self.rating}'
