{{ objname | escape | underline }}

.. currentmodule:: {{ module }}

.. autoclass:: {{ objname }}

   {% block attributes %}
   {% if attributes %}
   .. rubric:: {{ _('Attributes') }}

   .. autosummary::
   {% for item in attributes %}
      {%- if item not in inherited_members %}
      ~{{ name }}.{{ item }}
      {%- endif -%}
   {%- endfor %}
   {% endif %}
   {% endblock %}

   {% block methods %}
   {% if methods %}
   .. rubric:: {{ _('Methods') }}

   .. autosummary::
      :toctree:
   {% for item in methods %}
      {%- if item not in inherited_members %}
      ~{{ name }}.{{ item }}
      {%- endif -%}
   {%- endfor %}
   {% endif %}
   {% endblock %}


   {% block attribute_docs %}
   {% if attributes %}
   .. rubric:: Attributes Documentation
   {% for item in attributes %}
   {%- if item not in inherited_members %}
   .. autoattribute:: {{ name }}.{{ item }}
   {%- endif -%}
   {%- endfor %}
   {% endif %}
   {% endblock %}
