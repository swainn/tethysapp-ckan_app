from tethys_apps.base import TethysAppBase, url_map_maker


class CkanApp(TethysAppBase):
    """
    Tethys app class for Ckan App.
    """

    name = 'Ckan App'
    index = 'ckan_app:home'
    icon = 'ckan_app/images/icon.gif'
    package = 'ckan_app'
    root_url = 'ckan-app'
    color = '#1abc9c'
        
    def url_maps(self):
        """
        Add controllers
        """
        UrlMap = url_map_maker(self.root_url)

        url_maps = (UrlMap(name='home',
                           url='ckan-app',
                           controller='ckan_app.controllers.home'),
                    UrlMap(name='forecasts',
                           url='ckan-app/{watershed_dataset}/forecasts',
                           controller='ckan_app.controllers.forecasts'),
                    UrlMap(name='plot',
                           url='ckan-app/{resource_id}/plot',
                           controller='ckan_app.controllers.plot')
        )

        return url_maps