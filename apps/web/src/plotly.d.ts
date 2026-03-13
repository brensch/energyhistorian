declare module 'react-plotly.js/factory' {
  const createPlotlyComponent: (plotly: unknown) => any;
  export default createPlotlyComponent;
}

declare module 'plotly.js-basic-dist-min' {
  const Plotly: unknown;
  export default Plotly;
}
