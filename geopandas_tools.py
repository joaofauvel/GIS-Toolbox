from math import cos, sin, radians
from shapely import LineString, Point


def project(point: Point, distance: float, angle: float) -> Point:
    x1 = point.x + distance*sin(angle)
    y1 = point.y + distance*cos(angle)
    return Point(x1,y1)
    
def extend_point(point: Point, distance: float, angle: float) -> LineString:
    vertices = (
        point, 
        project(point, distance, angle)
    )
    return LineString(vertices)
    
def angle_at_vertex(line: LineString, vertex_index: int) -> float:
    '''Placeholder for now.'''
    pass
    
def extend_line(line: LineString, distance, angle = None) -> LineString:
    vertices = [v for v in line.coords]
    first_vertex = Point(vertices[0])
    last_vertex = Point(vertices[-1])
    dist_first_vertex, dist_last_vertex = distance if isinstance(distance, Iterable) else [distance/2]*2
    angle_at_first_vertex, angle_at_last_vertex = (angle_at_vertex(first_vertex),angle_at_vertex(last_vertex)) if not angle else angle
    vertices.insert(0, project(first_vertex,dist_first_vertex,angle_at_first_vertex))
    vertices.append(project(last_vertex,dist_last_vertex,angle_at_last_vertex))
    return LineString(vertices)
    
def centerline(point: Point, distance: float, angle: float) -> LineString:
    vertices = (
        project(point, distance/2, angle),
        point,
        project(point, -distance/2, angle)
    )
    return LineString(vertices)