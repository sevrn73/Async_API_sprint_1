from django.contrib import admin
from movies.models import Genre, Filmwork, Person, GenreFilmwork, PersonFilmwork


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork
    autocomplete_fields = ('genre',)


class PersonFilmworkkInline(admin.TabularInline):
    model = PersonFilmwork
    autocomplete_fields = ('person',)


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):

    list_display = (
        'name',
        'description',
    )
    search_fields = (
        'name',
        'description',
    )


@admin.register(Filmwork)
class FilmworkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline, PersonFilmworkkInline)

    list_display = (
        'title',
        'type',
        'creation_date',
        'rating',
    )

    list_filter = ('type',)

    search_fields = ('title', 'description', 'id')


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):

    list_display = ('full_name',)
    search_fields = ('full_name',)
