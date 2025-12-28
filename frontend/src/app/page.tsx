import { DatePicker } from "@/components/ui/date-picker";

export default function Home() {
  return (
    <div className="flex min-h-screen items-center justify-center bg-background p-8">
      <main className="flex flex-col items-center gap-8 text-center bg-card p-12 rounded-xl shadow-lg border border-border">
        <div className="space-y-2">
          <h1 className="text-3xl font-bold tracking-tighter sm:text-4xl md:text-5xl lg:text-6xl/none bg-gradient-to-r from-blue-600 to-violet-600 bg-clip-text text-transparent">
            Welcome to Shadcn
          </h1>
          <p className="mx-auto max-w-[700px] text-muted-foreground md:text-xl">
            Now featuring a fully functional Date Picker component.
          </p>
        </div>

        <div className="p-6 border border-border rounded-lg bg-background/50 backdrop-blur-sm">
          <DatePicker />
        </div>
      </main>
    </div>
  );
}
