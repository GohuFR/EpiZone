window.dashExtensions = Object.assign({}, window.dashExtensions, {
    default: {
        function0: function(f) {
            var p = f.properties;
            return {
                fillColor: p.color,
                color: p.color,
                weight: 0.7,
                opacity: 0.8,
                fillOpacity: 0.5
            };
        },
        function1: function(f, l) {
            var p = f.properties;
            l.bindTooltip('<b>' + p.commune + '</b> - ' + p.zone, {
                sticky: true,
                className: 'tip'
            });
            l.on('mouseover', function() {
                this.setStyle({
                    weight: 2.5,
                    color: '#fff',
                    fillOpacity: 0.72
                });
                this.bringToFront();
            });
            l.on('mouseout', function() {
                this.setStyle({
                    weight: 0.7,
                    color: p.color,
                    fillOpacity: 0.5
                });
            });
            l.on('click', function() {
                L.popup({
                        className: 'dark-popup'
                    }).setLatLng(l.getBounds().getCenter())
                    .setContent('<b>' + p.commune + '</b><br>INSEE : ' + p.code_insee + '<br>Dept : ' +
                        (p.departement || '') + '<br>Zone : <span style="color:' + p.color + ';font-weight:700">' + p.zone + '</span>')
                    .openOn(l._map);
            });
        },
        function2: function(f, ll) {
            var p = f.properties;
            if (p.type === 'foyer') return L.circleMarker(ll, {
                radius: 8,
                fillColor: '#f85149',
                color: '#f85149',
                fillOpacity: 0.9,
                weight: 2
            });
            return L.circleMarker(ll, {
                radius: 2,
                color: '#E65100',
                fillColor: '#FF6D00',
                fillOpacity: 0.5,
                stroke: false
            });
        },
        function3: function(f) {
            return {
                fillColor: '#E65100',
                color: '#E65100',
                weight: 0.7,
                opacity: 0.8,
                fillOpacity: 0.5
            };
        },
        function4: function(f, l) {
            var p = f.properties;
            l.bindTooltip(p.commune, {
                sticky: true
            });
            l.on('mouseover', function() {
                if (this.setStyle) {
                    this.setStyle({
                        weight: 2.5,
                        color: '#fff',
                        fillOpacity: 0.72
                    });
                    this.bringToFront();
                }
            });
            l.on('mouseout', function() {
                if (this.setStyle) this.setStyle({
                    weight: 0.7,
                    color: '#E65100',
                    fillOpacity: 0.5
                });
            });
        },
        function5: function(f, l) {
            var p = f.properties;
            if (p.type === 'foyer') l.bindPopup('<b>FOYER</b><br>' + p.commune);
        },
        function6: function(f, ll) {
            return L.marker(ll, {
                icon: L.divIcon({
                    className: 'region-label-marker',
                    html: '<div class="region-label">' + f.properties.label + '</div>',
                    iconSize: [0, 0],
                    iconAnchor: [0, 0]
                }),
                interactive: false,
                keyboard: false,
                zIndexOffset: -2000
            });
        },
        function7: function(f, context) {
                var code = f.properties.dept_code;
                var ho = context.hideout;
                if (code === ho.source_dept)
                    return {
                        fillColor: '#ff0000',
                        fillOpacity: 0.9,
                        color: '#ff6666',
                        weight: 2
                    };
                if (ho.vaccinated.indexOf(code) !== -1)
                    return {
                        fillColor: '#1565C0',
                        fillOpacity: 0.85,
                        color: '#90CAF9',
                        weight: 1.5
                    };
                if (ho.infected.indexOf(code) !== -1)
                    return {
                        fillColor: '#B71C1C',
                        fillOpacity: 0.85,
                        color: '#EF9A9A',
                        weight: 1
                    };
                if (ho.at_risk.indexOf(code) !== -1)
                    return {
                        fillColor: '#E65100',
                        fillOpacity: 0.5,
                        color: '#FFCC80',
                        weight: 1.5,
                        dashArray: '4 3'
                    };
                if (ho.blocked.indexOf(code) !== -1)
                    return {
                        fillColor: '#F57F17',
                        fillOpacity: 0.6,
                        color: '#FFF176',
                        weight: 2,
                        dashArray: '6 3'
                    };
                if (ho.resistant.indexOf(code) !== -1)
                    return {
                        fillColor: '#1B5E20',
                        fillOpacity: 0.4,
                        color: '#A5D6A7',
                        weight: 1
                    };
                if (ho.partial_resistant.indexOf(code) !== -1)
                    return {
                        fillColor: '#2E7D32',
                        fillOpacity: 0.2,
                        color: '#555',
                        weight: 0.8
                    };
                return {
                    fillColor: '#424242',
                    fillOpacity: 0.55,
                    color: '#666',
                    weight: 0.8
                };
            }

            ,
        function8: function(f, layer) {
                layer.bindTooltip(
                    f.properties.dept_nom + ' (' + f.properties.dept_code + ')', {
                        sticky: true,
                        className: 'game-tooltip'
                    }
                );
            }

            ,
        function9: function(f, ll) {
            return L.circleMarker(ll, {
                radius: 10,
                fillColor: '#ffffff',
                color: '#6a6af4',
                fillOpacity: 0.9,
                weight: 3
            });
        },
        function10: function(f, l) {
            l.bindTooltip('<b>' + f.properties.commune + '</b>', {
                permanent: true,
                direction: 'top',
                className: 'tip',
                offset: [0, -10]
            });
        }
    }
});